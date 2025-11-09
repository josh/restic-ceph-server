package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"regexp"
	"syscall"
	"time"

	"github.com/ceph/go-ceph/rados"
)

var verboseLog *log.Logger

var (
	errObjectNotFound    = errors.New("object not found")
	errObjectExists      = errors.New("object exists")
	errHashMismatch      = errors.New("hash mismatch")
	errWriteVerification = errors.New("write verification failed")
	errObjectTooLarge    = errors.New("object too large")
	hexBlobIDRegex       = regexp.MustCompile(`^[0-9a-fA-F]{64}$`)
	errClientAborted     = errors.New("client aborted request")
)

func initLogger(verbose bool, logFilePath string) error {
	logOutput := io.Writer(os.Stderr)

	if logFilePath != "" {
		file, err := os.OpenFile(logFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
		if err != nil {
			return fmt.Errorf("failed to open log file %s: %w", logFilePath, err)
		}
		logOutput = file
	}

	log.SetOutput(logOutput)
	log.SetFlags(0)

	if verbose {
		verboseLog = log.New(logOutput, "", 0)
	} else {
		verboseLog = log.New(io.Discard, "", 0)
	}

	return nil
}

type Config struct {
	Verbose         bool
	Listeners       listenerFlags
	UseStdio        bool
	ShutdownTimeout time.Duration
	AppendOnly      bool
	MaxIdleTime     time.Duration
	LogFile         string
	KeyringPath     string
	ClientID        string
}

func parseConfig() (Config, error) {
	var verbose bool
	var listeners listenerFlags
	var useStdio bool
	var shutdownTimeout time.Duration
	var appendOnly bool
	var maxIdleTime time.Duration
	var logFile string
	var keyringPath string
	var clientID string

	flag.BoolVar(&verbose, "v", false, "enable verbose logging")
	flag.BoolVar(&verbose, "verbose", false, "enable verbose logging")
	flag.Var(&listeners, "listen", "Address or Unix socket path to listen on, repeatable")
	flag.BoolVar(&useStdio, "stdio", false, "use HTTP/2 over stdin/stdout (default when no listeners specified)")
	flag.DurationVar(&shutdownTimeout, "shutdown-timeout", 30*time.Second, "graceful shutdown timeout for listeners")
	flag.BoolVar(&appendOnly, "append-only", false, "enable append-only mode (delete allowed for locks only)")
	flag.DurationVar(&maxIdleTime, "max-idle-time", 0, "exit after duration with no active connections (e.g., 30s, 5m; 0 = disabled)")
	flag.StringVar(&logFile, "log-file", "", "path to log file (default: stderr)")
	flag.StringVar(&keyringPath, "keyring", "", "path to Ceph keyring file")
	flag.StringVar(&clientID, "id", "", "Ceph client ID (e.g., 'restic' for client.restic)")
	flag.Parse()

	if keyringPath == "" {
		keyringPath = os.Getenv("CEPH_KEYRING")
	}

	if clientID == "" {
		clientID = os.Getenv("CEPH_ID")
	}

	return Config{
		Verbose:         verbose,
		Listeners:       listeners,
		UseStdio:        useStdio,
		ShutdownTimeout: shutdownTimeout,
		AppendOnly:      appendOnly,
		MaxIdleTime:     maxIdleTime,
		LogFile:         logFile,
		KeyringPath:     keyringPath,
		ClientID:        clientID,
	}, nil
}

func main() {
	config, err := parseConfig()
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}

	if err := initLogger(config.Verbose, config.LogFile); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}

	poolName := os.Getenv("CEPH_POOL")
	if poolName == "" {
		log.Printf("CEPH_POOL environment variable not set\n")
		os.Exit(1)
	}

	conn, err := setupCephConn(config)
	if err != nil {
		log.Printf("failed to setup Ceph connection: %v\n", err)
		os.Exit(1)
	}

	h := &Handler{
		conn:       conn,
		poolName:   poolName,
		appendOnly: config.AppendOnly,
	}

	mux := http.NewServeMux()
	h.setupRoutes(mux)

	ctx := context.Background()

	ctx, cancel := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	systemdSpecs, err := systemdListeners()
	if err != nil {
		log.Printf("failed to get systemd listeners: %v\n", err)
		os.Exit(1)
	}

	config.Listeners = append(config.Listeners, systemdSpecs...)
	if config.UseStdio && len(config.Listeners) > 0 {
		log.Printf("Error: --stdio cannot be combined with --listen\n")
		os.Exit(1)
	}
	hasConfiguredListeners := len(config.Listeners) > 0

	if !config.UseStdio && !hasConfiguredListeners {
		config.UseStdio = true
	}

	if config.UseStdio && config.MaxIdleTime > 0 {
		log.Printf("Error: --max-idle-time is not supported in stdio mode\n")
		os.Exit(1)
	}

	var monitor *idleMonitor
	if config.MaxIdleTime > 0 {
		monitor = newIdleMonitor(config.MaxIdleTime)
		defer monitor.Stop()
		go func() {
			select {
			case <-monitor.Done():
				cancel()
			case <-ctx.Done():
				monitor.Stop()
			}
		}()
	}

	if config.UseStdio {
		for _, cfg := range config.Listeners {
			cfg.Close()
		}

		stdioCfg := listenerConfig{
			kind: listenerTypeStdio,
			raw:  "stdio",
		}
		if err := stdioCfg.Serve(ctx, mux, config.ShutdownTimeout, monitor); err != nil && ctx.Err() == nil {
			log.Printf("stdio server error: %v\n", err)
			os.Exit(1)
		}
	} else {
		if err := serveAllListeners(ctx, cancel, config.Listeners, mux, config.ShutdownTimeout, monitor); err != nil {
			log.Printf("server error: %v\n", err)
			os.Exit(1)
		}
	}
}

func setupCephConn(config Config) (*rados.Conn, error) {
	var conn *rados.Conn
	var err error

	if config.ClientID != "" {
		conn, err = rados.NewConnWithUser(config.ClientID)
	} else {
		conn, err = rados.NewConn()
	}
	if err != nil {
		return nil, fmt.Errorf("failed to create RADOS connection: %v", err)
	}

	err = conn.ParseDefaultConfigEnv()
	if err != nil {
		return nil, fmt.Errorf("failed to parse CEPH_ARGS: %v", err)
	}

	if cephConf := os.Getenv("CEPH_CONF"); cephConf != "" {
		err = conn.ReadConfigFile(cephConf)
	} else {
		err = conn.ReadDefaultConfigFile()
	}
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %v", err)
	}

	if config.KeyringPath != "" {
		err = conn.SetConfigOption("keyring", config.KeyringPath)
		if err != nil {
			return nil, fmt.Errorf("failed to set keyring path: %v", err)
		}
	}

	err = conn.Connect()
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RADOS: %v", err)
	}

	return conn, nil
}
