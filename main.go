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
	"strconv"
	"sync"
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
	stripeSuffixLen              = 17
	defaultMaxObjectSize   int64 = 128 * 1024 * 1024
	defaultReadBufferSize  int64 = 16 * 1024 * 1024
	defaultWriteBufferSize int64 = 16 * 1024 * 1024
	defaultMaxWriteSize    int64 = 90 * 1024 * 1024
	defaultStripeCount     uint  = 1
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

func parseInt64Env(key string, defaultVal int64) int64 {
	val := os.Getenv(key)
	if val == "" {
		return defaultVal
	}
	parsed, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return defaultVal
	}
	return parsed
}

func parseUintEnv(key string, defaultVal uint) uint {
	val := os.Getenv(key)
	if val == "" {
		return defaultVal
	}
	parsed, err := strconv.ParseUint(val, 10, 64)
	if err != nil {
		return defaultVal
	}
	return uint(parsed)
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
	ReadBufferSize  int64
	WriteBufferSize int64
	MaxObjectSize   int64
	StripeUnit      uint
	StripeCount     uint
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
	var readBufferSize int64
	var writeBufferSize int64
	var maxObjectSize int64
	var stripeUnit uint
	var stripeCount uint

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
	flag.Int64Var(&readBufferSize, "read-buffer-size", defaultReadBufferSize, "buffer size for reading objects in bytes")
	flag.Int64Var(&writeBufferSize, "write-buffer-size", defaultWriteBufferSize, "buffer size for writing objects in bytes")
	flag.Int64Var(&maxObjectSize, "max-object-size", 0, "max object size override (0 = use cluster config or 128MB default)")
	flag.UintVar(&stripeUnit, "stripe-unit", 0, "striper stripe unit size (0 = use max object size)")
	flag.UintVar(&stripeCount, "stripe-count", defaultStripeCount, "striper stripe count")
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

	if readBufferSize == defaultReadBufferSize {
		readBufferSize = parseInt64Env("CEPH_SERVER_READ_BUFFER_SIZE", readBufferSize)
	}

	if writeBufferSize == defaultWriteBufferSize {
		writeBufferSize = parseInt64Env("CEPH_SERVER_WRITE_BUFFER_SIZE", writeBufferSize)
	}

	if maxObjectSize == 0 {
		maxObjectSize = parseInt64Env("CEPH_SERVER_MAX_OBJECT_SIZE", maxObjectSize)
	}

	if stripeUnit == 0 {
		stripeUnit = parseUintEnv("CEPH_SERVER_STRIPE_UNIT", stripeUnit)
	}

	if stripeCount == defaultStripeCount {
		stripeCount = parseUintEnv("CEPH_SERVER_STRIPE_COUNT", stripeCount)
	}

	if readBufferSize <= 0 {
		return Config{}, fmt.Errorf("read-buffer-size must be positive, got %d", readBufferSize)
	}

	if writeBufferSize <= 0 {
		return Config{}, fmt.Errorf("write-buffer-size must be positive, got %d", writeBufferSize)
	}

	if maxObjectSize < 0 {
		return Config{}, fmt.Errorf("max-object-size cannot be negative, got %d", maxObjectSize)
	}

	if stripeCount == 0 {
		return Config{}, fmt.Errorf("stripe-count must be positive, got %d", stripeCount)
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
		ReadBufferSize:  readBufferSize,
		WriteBufferSize: writeBufferSize,
		MaxObjectSize:   maxObjectSize,
		StripeUnit:      stripeUnit,
		StripeCount:     stripeCount,
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
		PoolName:      config.PoolName,
		KeyringPath:   config.KeyringPath,
		ClientID:      config.ClientID,
		CephConf:      config.CephConf,
		MaxObjectSize: config.MaxObjectSize,
		StripeUnit:    config.StripeUnit,
		StripeCount:   config.StripeCount,
	}

	connMgr := NewConnectionManager(cephConfig)
	defer connMgr.Shutdown()

	maxWriteSize, err := connMgr.GetMaxWriteSize()
	if err != nil {
		slog.Warn("failed to get max write size for validation", "error", err)
	} else if config.WriteBufferSize > maxWriteSize {
		slog.Warn("write buffer size exceeds cluster max write size, writes may be chunked or fail",
			"write_buffer_size", config.WriteBufferSize,
			"cluster_max_write_size", maxWriteSize)
	}

	h := &Handler{
		connMgr:         connMgr,
		appendOnly:      config.AppendOnly,
		striperEnabled:  config.EnableStriper,
		readBufferSize:  config.ReadBufferSize,
		writeBufferSize: config.WriteBufferSize,
		readBufferPool: &sync.Pool{
			New: func() interface{} {
				buf := make([]byte, config.ReadBufferSize)
				return &buf
			},
		},
		writeBufferPool: &sync.Pool{
			New: func() interface{} {
				buf := make([]byte, config.WriteBufferSize)
				return &buf
			},
		},
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
