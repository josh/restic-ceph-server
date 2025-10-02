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

	"github.com/ceph/go-ceph/rados"
	"golang.org/x/net/http2"
)

var (
	radosConn *rados.Conn
	connOnce  sync.Once
	connErr   error
)

func main() {
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

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
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
