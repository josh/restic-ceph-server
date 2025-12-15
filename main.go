package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"regexp"
	"syscall"
	"time"
)

var logger *slog.Logger

var (
	errObjectNotFound       = errors.New("object not found")
	errObjectExists         = errors.New("object exists")
	errHashMismatch         = errors.New("hash mismatch")
	hexBlobIDRegex          = regexp.MustCompile(`^[0-9a-fA-F]{64}$`)
	stripedBlobIDRegex      = regexp.MustCompile(`^[0-9a-fA-F]{64}\.[0-9a-f]{16}$`)
	firstStripedBlobIDRegex = regexp.MustCompile(`^[0-9a-fA-F]{64}\.0000000000000000$`)
	errClientAborted        = errors.New("client aborted request")
)

const (
	stripeSuffixLen             = 17
	defaultMaxObjectSize  int64 = 128 * 1024 * 1024
	defaultWriteChunkSize       = 16 * 1024 * 1024
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

	logLevel := slog.LevelInfo
	if verbose {
		logLevel = slog.LevelDebug
	}

	handler := slog.NewTextHandler(logOutput, &slog.HandlerOptions{
		Level: logLevel,
	})
	logger = slog.New(handler)
	slog.SetDefault(logger)

	return nil
}

func parseBoolEnv(key string) bool {
	val := os.Getenv(key)
	return val == "true" || val == "1" || val == "yes"
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
	PoolName        string
	CephConf        string
	EnableStriper   bool
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
	var poolName string
	var cephConf string
	var enableStriper bool

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
	flag.StringVar(&poolName, "pool", "", "Ceph pool name")
	flag.StringVar(&cephConf, "ceph-conf", "", "path to ceph.conf file")
	flag.BoolVar(&enableStriper, "enable-striper", false, "use librados striper for large objects")
	flag.Parse()

	if !verbose {
		verbose = parseBoolEnv("CEPH_SERVER_VERBOSE")
	}

	if !appendOnly {
		appendOnly = parseBoolEnv("CEPH_SERVER_APPEND_ONLY")
	}

	if !enableStriper {
		enableStriper = parseBoolEnv("CEPH_SERVER_ENABLE_STRIPER")
	}

	if logFile == "" {
		logFile = os.Getenv("CEPH_SERVER_LOG_FILE")
	}

	if keyringPath == "" {
		keyringPath = os.Getenv("CEPH_KEYRING")
	}

	if clientID == "" {
		clientID = os.Getenv("CEPH_ID")
	}

	if poolName == "" {
		poolName = os.Getenv("CEPH_POOL")
	}

	if cephConf == "" {
		cephConf = os.Getenv("CEPH_CONF")
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
		PoolName:        poolName,
		CephConf:        cephConf,
		EnableStriper:   enableStriper,
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

	if config.PoolName == "" {
		slog.Error("Ceph pool name not set (use --pool or CEPH_POOL)")
		os.Exit(1)
	}

	cephConfig := CephConfig{
		PoolName:    config.PoolName,
		KeyringPath: config.KeyringPath,
		ClientID:    config.ClientID,
		CephConf:    config.CephConf,
	}

	connMgr := NewConnectionManager(cephConfig, logger)
	defer connMgr.Shutdown()

	h := &Handler{
		connMgr:        connMgr,
		appendOnly:     config.AppendOnly,
		logger:         logger,
		striperEnabled: config.EnableStriper,
	}

	mux := http.NewServeMux()
	h.setupRoutes(mux)

	ctx := context.Background()

	ctx, cancel := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	systemdSpecs, err := systemdListeners()
	if err != nil {
		slog.Error("failed to get systemd listeners", "error", err)
		os.Exit(1)
	}

	config.Listeners = append(config.Listeners, systemdSpecs...)
	if config.UseStdio && len(config.Listeners) > 0 {
		slog.Error("--stdio cannot be combined with --listen")
		os.Exit(1)
	}
	hasConfiguredListeners := len(config.Listeners) > 0

	if !config.UseStdio && !hasConfiguredListeners {
		config.UseStdio = true
	}

	if config.UseStdio && config.MaxIdleTime > 0 {
		slog.Error("--max-idle-time is not supported in stdio mode")
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
			slog.Error("stdio server error", "error", err)
			os.Exit(1)
		}
	} else {
		if err := serveAllListeners(ctx, cancel, config.Listeners, mux, config.ShutdownTimeout, monitor); err != nil {
			slog.Error("server error", "error", err)
			os.Exit(1)
		}
	}
}
