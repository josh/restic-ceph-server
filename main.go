package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"regexp"
	"sync"
	"syscall"
	"time"

	"github.com/ceph/go-ceph/rados"
	"golang.org/x/net/http2"
)

var (
	radosConn *rados.Conn
	connOnce  sync.Once
	connErr   error
)

type Config struct {
	Deadline *time.Time
}

func parseConfig() (Config, error) {
	var deadline *time.Time

	if envDeadline := os.Getenv("RESTIC_CEPH_SERVER_DEADLINE"); envDeadline != "" {
		if parsed, err := time.Parse(time.RFC3339, envDeadline); err == nil {
			deadline = &parsed
		} else {
			return Config{}, fmt.Errorf("invalid RESTIC_CEPH_SERVER_DEADLINE value: %s", envDeadline)
		}
	}

	return Config{
		Deadline: deadline,
	}, nil
}

func main() {
	config, err := parseConfig()
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}

	handler := http.NewServeMux()

	handler.HandleFunc("/config", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(os.Stderr, "%v %v\n", r.Method, r.URL)

		if r.Method == "HEAD" || r.Method == "GET" {
			// w.Header().Add("Content-Length", "0")
			http.Error(w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
			return
		}

		http.Error(w, http.StatusText(http.StatusNotImplemented), http.StatusNotImplemented)
	})

	handler.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		// Restic sends a preflight /file-123 test request, ignore it
		fileTestRegex := regexp.MustCompile(`^/file-\d+$`)
		if r.Method == "GET" && fileTestRegex.MatchString(r.URL.Path) {
			http.NotFound(w, r)
			return
		}

		fmt.Fprintf(os.Stderr, "%v %v\n", r.Method, r.URL)

		poolName := os.Getenv("CEPH_POOL")
		if poolName == "" {
			fmt.Fprintf(os.Stderr, "CEPH_POOL environment variable not set\n")
			http.NotFound(w, r)
			return
		}

		conn, err := getCephConnection()
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to get Ceph connection: %v\n", err)
			http.Error(w, "internal server error", http.StatusInternalServerError)
			return
		}

		_, err = conn.GetPoolByName(poolName)
		if err != nil {
			fmt.Fprintf(os.Stderr, "pool check failed: pool '%s' does not exist: %v\n", poolName, err)
			http.NotFound(w, r)
			return
		}

		if r.Method == "POST" && r.URL.Path == "/" && r.URL.Query().Get("create") == "true" {
			// w.WriteHeader(http.StatusOK)
			http.NotFound(w, r)
			return
		}

		http.Error(w, http.StatusText(http.StatusNotImplemented), http.StatusNotImplemented)
	})

	ctx := context.Background()

	if config.Deadline != nil {
		var deadlineCancel context.CancelFunc
		ctx, deadlineCancel = context.WithDeadline(ctx, *config.Deadline)
		defer deadlineCancel()
	}

	ctx, cancel := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	server := &http2.Server{}

	stdioConn := &StdioConn{
		stdin:  os.Stdin,
		stdout: os.Stdout,
	}

	server.ServeConn(stdioConn, &http2.ServeConnOpts{
		Context: ctx,
		Handler: handler,
	})

	if ctx.Err() == context.DeadlineExceeded {
		fmt.Fprintf(os.Stderr, "Server terminated due to deadline\n")
		os.Exit(1)
	}
}

func getCephConnection() (*rados.Conn, error) {
	connOnce.Do(func() {
		radosConn, connErr = setupCephConn()
	})
	return radosConn, connErr
}

func setupCephConn() (*rados.Conn, error) {
	conn, err := rados.NewConn()
	if err != nil {
		return nil, fmt.Errorf("failed to create RADOS connection: %v", err)
	}

	if cephConf := os.Getenv("CEPH_CONF"); cephConf != "" {
		err = conn.ReadConfigFile(cephConf)
	} else {
		err = conn.ReadDefaultConfigFile()
	}
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %v", err)
	}

	err = conn.Connect()
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RADOS: %v", err)
	}

	return conn, nil
}
