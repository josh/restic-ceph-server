package main

import (
	"fmt"
	"net/http"
	"os"
	"regexp"
	"time"

	"github.com/ceph/go-ceph/rados"
	"golang.org/x/net/http2"
)

func main() {
	handler := http.NewServeMux()

	_, err := setupCephConn()
	if err != nil {
		fmt.Fprintf(os.Stderr, "warning: %v\n", err)
	}

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

		if r.Method == "POST" && r.URL.Path == "/" && r.URL.Query().Get("create") == "true" {
			// w.WriteHeader(http.StatusOK)
			http.NotFound(w, r)
			return
		}

		http.Error(w, http.StatusText(http.StatusNotImplemented), http.StatusNotImplemented)
	})

	// Test timeout after 5 seconds
	go func() {
		time.Sleep(5 * time.Second)
		os.Exit(0)
	}()

	server := &http2.Server{}

	conn := &StdioConn{
		stdin:  os.Stdin,
		stdout: os.Stdout,
	}

	server.ServeConn(conn, &http2.ServeConnOpts{
		Handler: handler,
	})
}

func setupCephConn() (*rados.Conn, error) {
	conn, err := rados.NewConn()
	if err != nil {
		return nil, err
	}
	err = conn.ReadDefaultConfigFile()
	if err != nil {
		return nil, err
	}
	err = conn.Connect()
	if err != nil {
		return nil, err
	}
	return conn, nil
}
