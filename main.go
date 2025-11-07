package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"mime"
	"net"
	"net/http"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"strings"
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

var (
	maxObjectSize     int64
	maxObjectSizeOnce sync.Once
	maxObjectSizeErr  error
)

var verboseLog *log.Logger

var (
	errObjectNotFound    = errors.New("object not found")
	errObjectExists      = errors.New("object exists")
	errHashMismatch      = errors.New("hash mismatch")
	errWriteVerification = errors.New("write verification failed")
	errObjectTooLarge    = errors.New("object too large")
	hexBlobIDRegex       = regexp.MustCompile(`^[0-9a-fA-F]+$`)
)

type addrList []string

func (a *addrList) String() string {
	return strings.Join(*a, ",")
}

func (a *addrList) Set(value string) error {
	*a = append(*a, value)
	return nil
}

type Handler struct {
	conn       *rados.Conn
	poolName   string
	appendOnly bool
}

func (h *Handler) openIOContext() (*rados.IOContext, error) {
	return h.conn.OpenIOContext(h.poolName)
}

func isValidBlobType(blobType string) bool {
	switch blobType {
	case "keys", "locks", "snapshots", "data", "index":
		return true
	default:
		return false
	}
}

type errorCoder interface {
	ErrorCode() int
}

func (h *Handler) handleRadosError(w http.ResponseWriter, r *http.Request, object string, err error) {
	var opErr rados.OperationError
	if errors.As(err, &opErr) && opErr.OpError != nil {
		if ec, ok := opErr.OpError.(errorCoder); ok {
			switch ec.ErrorCode() {
			case -int(syscall.EFBIG):
				http.Error(w, "object size exceeds cluster limit", http.StatusRequestEntityTooLarge)
				return
			case -int(syscall.ENOSPC):
				http.Error(w, "insufficient storage", http.StatusInsufficientStorage)
				return
			case -int(syscall.EDQUOT):
				http.Error(w, "insufficient storage", http.StatusInsufficientStorage)
				return
			}
		}
	}

	switch {
	case errors.Is(err, errObjectNotFound):
		http.NotFound(w, r)
	case errors.Is(err, errObjectExists):
		http.Error(w, "object already exists", http.StatusForbidden)
	case errors.Is(err, errHashMismatch):
		http.Error(w, "hash mismatch", http.StatusBadRequest)
	case errors.Is(err, errWriteVerification):
		http.Error(w, "write verification failed", http.StatusInternalServerError)
	case errors.Is(err, errObjectTooLarge):
		http.Error(w, "object size exceeds cluster limit", http.StatusRequestEntityTooLarge)
	default:
		log.Printf("failed to serve %s: %v\n", object, err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
	}
}

func (h *Handler) checkConfig(w http.ResponseWriter, r *http.Request) {
	verboseLog.Printf("%v %v\n", r.Method, r.URL)

	ioctx, err := h.openIOContext()
	if err != nil {
		if errors.Is(err, rados.ErrNotFound) {
			http.NotFound(w, r)
			return
		}
		log.Printf("failed to open IO context: %v\n", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}
	defer ioctx.Destroy()

	if err := serveRadosObjectWithRequest(w, r, ioctx, "config", true); err != nil {
		h.handleRadosError(w, r, "config", err)
	}
}

func (h *Handler) getConfig(w http.ResponseWriter, r *http.Request) {
	verboseLog.Printf("%v %v\n", r.Method, r.URL)

	ioctx, err := h.openIOContext()
	if err != nil {
		if errors.Is(err, rados.ErrNotFound) {
			http.NotFound(w, r)
			return
		}
		log.Printf("failed to open IO context: %v\n", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}
	defer ioctx.Destroy()

	if err := serveRadosObjectWithRequest(w, r, ioctx, "config", false); err != nil {
		h.handleRadosError(w, r, "config", err)
	}
}

func (h *Handler) saveConfig(w http.ResponseWriter, r *http.Request) {
	verboseLog.Printf("%v %v\n", r.Method, r.URL)

	ioctx, err := h.openIOContext()
	if err != nil {
		if errors.Is(err, rados.ErrNotFound) {
			http.NotFound(w, r)
			return
		}
		log.Printf("failed to open IO context: %v\n", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}
	defer ioctx.Destroy()

	if err := createRadosObject(w, r, ioctx, "config", "config"); err != nil {
		h.handleRadosError(w, r, "config", err)
	}
}

func (h *Handler) deleteConfig(w http.ResponseWriter, r *http.Request) {
	verboseLog.Printf("%v %v\n", r.Method, r.URL)

	if h.appendOnly {
		verboseLog.Printf("delete blocked in append-only mode for config\n")
		http.Error(w, "delete not allowed in append-only mode", http.StatusForbidden)
		return
	}

	ioctx, err := h.openIOContext()
	if err != nil {
		if errors.Is(err, rados.ErrNotFound) {
			http.NotFound(w, r)
			return
		}
		log.Printf("failed to open IO context: %v\n", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}
	defer ioctx.Destroy()

	if err := deleteRadosObject(w, ioctx, "config"); err != nil {
		h.handleRadosError(w, r, "config", err)
	}
}

func (h *Handler) createRepo(w http.ResponseWriter, r *http.Request) {
	verboseLog.Printf("%v %v\n", r.Method, r.URL)

	_, err := h.conn.GetPoolByName(h.poolName)
	if err != nil {
		log.Printf("pool check failed: pool '%s' does not exist: %v\n", h.poolName, err)
		http.NotFound(w, r)
		return
	}

	createParam := r.URL.Query().Get("create")
	if createParam == "" {
		http.Error(w, "missing required query parameter: create", http.StatusBadRequest)
		return
	}
	if createParam != "true" {
		http.Error(w, "invalid value for create parameter: must be 'true'", http.StatusBadRequest)
		return
	}

	ioctx, err := h.openIOContext()
	if err != nil {
		log.Printf("failed to open IO context: %v\n", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}
	defer ioctx.Destroy()

	w.WriteHeader(http.StatusOK)
}

func initLogger(verbose bool) error {
	logOutput := io.Writer(os.Stderr)

	logFilePath := os.Getenv("__RESTIC_CEPH_SERVER_LOG_FILE")
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
	Deadline *time.Time
}

func parseConfig() (Config, error) {
	var deadline *time.Time

	if envDeadline := os.Getenv("__RESTIC_CEPH_SERVER_DEADLINE"); envDeadline != "" {
		if parsed, err := time.Parse(time.RFC3339, envDeadline); err == nil {
			deadline = &parsed
		} else {
			return Config{}, fmt.Errorf("invalid __RESTIC_CEPH_SERVER_DEADLINE value: %s", envDeadline)
		}
	}

	return Config{
		Deadline: deadline,
	}, nil
}

func main() {
	var verbose bool
	var socketPath string
	var addrs addrList
	var useStdio bool
	var shutdownTimeout time.Duration
	var appendOnly bool
	flag.BoolVar(&verbose, "v", false, "enable verbose logging")
	flag.BoolVar(&verbose, "verbose", false, "enable verbose logging")
	flag.StringVar(&socketPath, "socket", "", "Unix socket path to listen on")
	flag.Var(&addrs, "addr", "TCP address to listen on (host:port), repeatable for multiple interfaces")
	flag.BoolVar(&useStdio, "stdio", false, "use HTTP/2 over stdin/stdout (default when no listeners specified)")
	flag.DurationVar(&shutdownTimeout, "shutdown-timeout", 30*time.Second, "graceful shutdown timeout for listeners")
	flag.BoolVar(&appendOnly, "append-only", false, "enable append-only mode (delete allowed for locks only)")
	flag.Parse()

	if !verbose {
		envVerbose := os.Getenv("__RESTIC_CEPH_VERBOSE")
		verbose = envVerbose == "1" || strings.EqualFold(envVerbose, "true") || strings.EqualFold(envVerbose, "yes")
	}

	if useStdio && (socketPath != "" || len(addrs) > 0) {
		log.Printf("Error: --stdio cannot be combined with --socket or --addr\n")
		os.Exit(1)
	}

	for _, addr := range addrs {
		if !strings.Contains(addr, ":") {
			log.Printf("Error: invalid --addr format '%s': must specify port (e.g., :8080 or 127.0.0.1:8080)\n", addr)
			os.Exit(1)
		}
	}

	if err := initLogger(verbose); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}

	config, err := parseConfig()
	if err != nil {
		log.Printf("%v\n", err)
		os.Exit(1)
	}

	poolName := os.Getenv("CEPH_POOL")
	if poolName == "" {
		log.Printf("CEPH_POOL environment variable not set\n")
		os.Exit(1)
	}

	conn, err := getCephConnection()
	if err != nil {
		log.Printf("failed to get Ceph connection: %v\n", err)
		os.Exit(1)
	}

	h := &Handler{
		conn:       conn,
		poolName:   poolName,
		appendOnly: appendOnly,
	}

	mux := http.NewServeMux()

	mux.HandleFunc("HEAD /config", h.checkConfig)
	mux.HandleFunc("GET /config", h.getConfig)
	mux.HandleFunc("POST /config", h.saveConfig)
	mux.HandleFunc("DELETE /config", h.deleteConfig)

	mux.HandleFunc("GET /{type}/", h.listBlobs)
	mux.HandleFunc("HEAD /{type}/{id}", h.checkBlob)
	mux.HandleFunc("GET /{type}/{id}", h.getBlob)
	mux.HandleFunc("POST /{type}/{id}", h.saveBlob)
	mux.HandleFunc("DELETE /{type}/{id}", h.deleteBlob)

	mux.HandleFunc("POST /", h.createRepo)

	ctx := context.Background()

	if config.Deadline != nil {
		var deadlineCancel context.CancelFunc
		ctx, deadlineCancel = context.WithDeadline(ctx, *config.Deadline)
		defer deadlineCancel()
	}

	ctx, cancel := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	systemdListeners, err := systemdListeners()
	if err != nil {
		log.Printf("failed to get systemd listeners: %v\n", err)
		os.Exit(1)
	}

	hasListeners := socketPath != "" || len(addrs) > 0 || len(systemdListeners) > 0

	if useStdio || !hasListeners {
		server := &http2.Server{}

		stdioConn := &StdioConn{
			stdin:  os.Stdin,
			stdout: os.Stdout,
		}

		server.ServeConn(stdioConn, &http2.ServeConnOpts{
			Context: ctx,
			Handler: mux,
		})
	} else {
		var wg sync.WaitGroup
		errChan := make(chan error, 1+len(addrs)+len(systemdListeners))

		if socketPath != "" {
			wg.Add(1)
			go func() {
				defer wg.Done()
				log.Printf("Listening on unix://%s\n", socketPath)
				if err := serveUnixSocket(ctx, socketPath, mux, shutdownTimeout); err != nil && ctx.Err() == nil {
					select {
					case errChan <- fmt.Errorf("unix socket error: %w", err):
					default:
					}
					cancel()
				}
			}()
		}

		for i, listener := range systemdListeners {
			wg.Add(1)
			go func(idx int, l net.Listener) {
				defer wg.Done()
				log.Printf("Listening on systemd socket %s (%s)\n", l.Addr().String(), l.Addr().Network())
				if err := serveSystemdListener(ctx, l, mux, shutdownTimeout); err != nil && ctx.Err() == nil {
					select {
					case errChan <- fmt.Errorf("systemd listener %d error: %w", idx, err):
					default:
					}
					cancel()
				}
			}(i, listener)
		}

		for _, addr := range addrs {
			wg.Add(1)
			go func() {
				defer wg.Done()
				log.Printf("Listening on %s\n", addr)
				if err := serveTCPListener(ctx, addr, mux, shutdownTimeout); err != nil && ctx.Err() == nil {
					select {
					case errChan <- fmt.Errorf("TCP listener %s error: %w", addr, err):
					default:
					}
					cancel()
				}
			}()
		}

		wg.Wait()
		close(errChan)

		if err := <-errChan; err != nil {
			log.Printf("server error: %v\n", err)
			os.Exit(1)
		}
	}

	if ctx.Err() == context.DeadlineExceeded {
		log.Printf("Server terminated due to deadline\n")
		os.Exit(1)
	}
}

const listenFdsStart = 3

func systemdFiles(unsetEnv bool) []*os.File {
	if unsetEnv {
		defer func() {
			_ = os.Unsetenv("LISTEN_PID")
			_ = os.Unsetenv("LISTEN_FDS")
			_ = os.Unsetenv("LISTEN_FDNAMES")
		}()
	}

	pid, err := strconv.Atoi(os.Getenv("LISTEN_PID"))
	if err != nil || pid != os.Getpid() {
		return nil
	}

	nfds, err := strconv.Atoi(os.Getenv("LISTEN_FDS"))
	if err != nil || nfds <= 0 {
		return nil
	}

	names := strings.Split(os.Getenv("LISTEN_FDNAMES"), ":")

	files := make([]*os.File, 0, nfds)
	for fd := listenFdsStart; fd < listenFdsStart+nfds; fd++ {
		syscall.CloseOnExec(fd)
		name := "LISTEN_FD_" + strconv.Itoa(fd)
		offset := fd - listenFdsStart
		if offset < len(names) && len(names[offset]) > 0 {
			name = names[offset]
		}
		files = append(files, os.NewFile(uintptr(fd), name))
	}

	return files
}

func systemdListeners() ([]net.Listener, error) {
	files := systemdFiles(true)
	listeners := make([]net.Listener, 0, len(files))

	for _, f := range files {
		if listener, err := net.FileListener(f); err == nil {
			listeners = append(listeners, listener)
			_ = f.Close()
		}
	}

	return listeners, nil
}

func serveListener(ctx context.Context, listener net.Listener, handler http.Handler, shutdownTimeout time.Duration) error {
	server := &http.Server{
		Handler: handler,
	}

	_ = http2.ConfigureServer(server, &http2.Server{})

	errChan := make(chan error, 1)
	go func() {
		errChan <- server.Serve(listener)
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer shutdownCancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			log.Printf("server shutdown error: %v\n", err)
		}
		return ctx.Err()
	case err := <-errChan:
		if err != nil && err != http.ErrServerClosed {
			return err
		}
		return nil
	}
}

func serveUnixSocket(ctx context.Context, socketPath string, handler http.Handler, shutdownTimeout time.Duration) error {
	if err := os.Remove(socketPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to remove existing socket: %w", err)
	}

	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		return fmt.Errorf("failed to create Unix socket listener: %w", err)
	}
	defer func() { _ = listener.Close() }()
	defer func() { _ = os.Remove(socketPath) }()

	return serveListener(ctx, listener, handler, shutdownTimeout)
}

func serveTCPListener(ctx context.Context, addr string, handler http.Handler, shutdownTimeout time.Duration) error {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to create TCP listener: %w", err)
	}
	defer func() { _ = listener.Close() }()

	return serveListener(ctx, listener, handler, shutdownTimeout)
}

func serveSystemdListener(ctx context.Context, listener net.Listener, handler http.Handler, shutdownTimeout time.Duration) error {
	defer func() { _ = listener.Close() }()
	return serveListener(ctx, listener, handler, shutdownTimeout)
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

func getMaxObjectSize() (int64, error) {
	maxObjectSizeOnce.Do(func() {
		conn, err := getCephConnection()
		if err != nil {
			maxObjectSizeErr = fmt.Errorf("failed to get connection: %w", err)
			return
		}

		sizeStr, err := conn.GetConfigOption("osd_max_object_size")
		if err != nil {
			maxObjectSizeErr = fmt.Errorf("failed to read osd_max_object_size: %w", err)
			return
		}

		size, err := strconv.ParseInt(sizeStr, 10, 64)
		if err != nil {
			maxObjectSizeErr = fmt.Errorf("invalid osd_max_object_size value %q: %w", sizeStr, err)
			return
		}

		maxObjectSize = size
	})
	return maxObjectSize, maxObjectSizeErr
}

const radosReadChunkSize = 32 * 1024

func expectedHash(object string) ([32]byte, error) {
	if object == "config" {
		return [32]byte{}, nil
	}

	hashBytes, err := hex.DecodeString(object)
	if err != nil {
		return [32]byte{}, fmt.Errorf("invalid hash format: %w", err)
	}
	if len(hashBytes) != 32 {
		return [32]byte{}, fmt.Errorf("invalid hash length: expected 32 bytes, got %d", len(hashBytes))
	}

	return [32]byte(hashBytes), nil
}

func serveRadosObjectWithRequest(w http.ResponseWriter, r *http.Request, ioctx *rados.IOContext, object string, head bool) error {
	stat, err := ioctx.Stat(object)
	if err != nil {
		if errors.Is(err, rados.ErrNotFound) {
			return errObjectNotFound
		}
		return fmt.Errorf("stat %s: %w", object, err)
	}

	if stat.Size > uint64(math.MaxInt64) {
		return fmt.Errorf("object %s size exceeds max int64: %d", object, stat.Size)
	}

	size := int64(stat.Size)
	start := int64(0)
	end := size - 1
	status := http.StatusOK

	rangeHeader := ""
	if r != nil {
		rangeHeader = r.Header.Get("Range")
	}

	if rangeHeader != "" {
		var rangeStart, rangeEnd int64

		if !strings.HasPrefix(rangeHeader, "bytes=") {
			http.Error(w, "only bytes ranges are supported", http.StatusRequestedRangeNotSatisfiable)
			return fmt.Errorf("unsupported range unit in: %s", rangeHeader)
		}

		rangeSpec := strings.TrimPrefix(rangeHeader, "bytes=")

		if strings.Contains(rangeSpec, ",") {
			http.Error(w, "multiple ranges not supported", http.StatusRequestedRangeNotSatisfiable)
			return fmt.Errorf("multiple ranges not supported: %s", rangeHeader)
		}

		parts := strings.Split(rangeSpec, "-")
		if len(parts) != 2 {
			http.Error(w, "invalid range format", http.StatusRequestedRangeNotSatisfiable)
			return fmt.Errorf("invalid range format: %s", rangeHeader)
		}

		if parts[0] == "" && parts[1] == "" {
			http.Error(w, "invalid range format", http.StatusRequestedRangeNotSatisfiable)
			return fmt.Errorf("empty range spec: %s", rangeHeader)
		}

		if parts[0] == "" {
			suffixLength, err := strconv.ParseInt(parts[1], 10, 64)
			if err != nil || suffixLength < 0 {
				http.Error(w, "invalid suffix length", http.StatusRequestedRangeNotSatisfiable)
				return fmt.Errorf("invalid suffix length in range: %s", rangeHeader)
			}
			if suffixLength >= size {
				start = 0
			} else {
				start = size - suffixLength
			}
			end = size - 1
		} else {
			rangeStart, err = strconv.ParseInt(parts[0], 10, 64)
			if err != nil || rangeStart < 0 {
				http.Error(w, "invalid range start", http.StatusRequestedRangeNotSatisfiable)
				return fmt.Errorf("invalid range start: %w", err)
			}

			if rangeStart >= size {
				http.Error(w, "range not satisfiable", http.StatusRequestedRangeNotSatisfiable)
				return fmt.Errorf("range start %d out of bounds for size %d", rangeStart, size)
			}

			start = rangeStart

			if parts[1] != "" {
				rangeEnd, err = strconv.ParseInt(parts[1], 10, 64)
				if err != nil || rangeEnd < 0 {
					http.Error(w, "invalid range end", http.StatusRequestedRangeNotSatisfiable)
					return fmt.Errorf("invalid range end: %w", err)
				}
				if rangeEnd >= size {
					rangeEnd = size - 1
				}
				end = rangeEnd
			} else {
				end = size - 1
			}

			if start > end {
				http.Error(w, "range not satisfiable", http.StatusRequestedRangeNotSatisfiable)
				return fmt.Errorf("range start %d greater than end %d", start, end)
			}
		}

		status = http.StatusPartialContent
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, size))
	}

	contentLength := end - start + 1
	w.Header().Set("Content-Length", strconv.FormatInt(contentLength, 10))
	w.WriteHeader(status)

	if head || contentLength == 0 {
		return nil
	}

	buffer := make([]byte, radosReadChunkSize)
	remaining := contentLength
	offset := start

	for remaining > 0 {
		chunkSize := len(buffer)
		if remaining < int64(chunkSize) {
			chunkSize = int(remaining)
		}

		n, err := ioctx.Read(object, buffer[:chunkSize], uint64(offset))
		if err != nil {
			if errors.Is(err, rados.ErrNotFound) {
				return errObjectNotFound
			}
			return fmt.Errorf("read %s: %w", object, err)
		}
		if n == 0 {
			return fmt.Errorf("short read on %s", object)
		}

		if _, err := w.Write(buffer[:n]); err != nil {
			return fmt.Errorf("write response: %w", err)
		}

		offset += int64(n)
		remaining -= int64(n)
	}

	return nil
}

func createRadosObject(w http.ResponseWriter, r *http.Request, ioctx *rados.IOContext, object string, hashID string) error {
	maxSize, err := getMaxObjectSize()
	if err != nil {
		return fmt.Errorf("failed to get max object size: %w", err)
	}

	if r.ContentLength > 0 && r.ContentLength > maxSize {
		return errObjectTooLarge
	}

	data := make([]byte, 0, 4096)
	buffer := make([]byte, 4096)

	for {
		n, err := r.Body.Read(buffer)
		if n > 0 {
			data = append(data, buffer[:n]...)
		}
		if err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("read request body: %w", err)
		}
	}

	expected, err := expectedHash(hashID)
	if err != nil {
		return err
	}

	if expected != [32]byte{} {
		actual := sha256.Sum256(data)
		if actual != expected {
			log.Printf("input hash mismatch for %s: expected %x, got %x\n", object, expected, actual)
			return errHashMismatch
		}
	}

	_, err = ioctx.Stat(object)
	if err == nil {
		return errObjectExists
	}
	if !errors.Is(err, rados.ErrNotFound) {
		return fmt.Errorf("stat object %s: %w", object, err)
	}

	writeOp := rados.CreateWriteOp()
	defer writeOp.Release()

	writeOp.Create(rados.CreateExclusive)
	writeOp.SetAllocationHint(uint64(len(data)), uint64(len(data)), rados.AllocHintIncompressible|rados.AllocHintImmutable|rados.AllocHintLonglived)
	writeOp.WriteFull(data)

	err = writeOp.Operate(ioctx, object, rados.OperationNoFlag)
	if err != nil {
		return fmt.Errorf("write object %s: %w", object, err)
	}

	if expected != [32]byte{} {
		readData := make([]byte, len(data))
		_, err = ioctx.Read(object, readData, 0)
		if err != nil {
			return fmt.Errorf("read object %s after write: %w", object, err)
		}

		actual := sha256.Sum256(readData)

		if actual != expected {
			if err := ioctx.Delete(object); err != nil {
				log.Printf("failed to delete object %s after write verification failure: %v\n", object, err)
			}
			log.Printf("write verification failed for %s: expected %x, got %x\n", object, expected, actual)
			return errWriteVerification
		}
	}

	w.WriteHeader(http.StatusOK)
	return nil
}

func deleteRadosObject(w http.ResponseWriter, ioctx *rados.IOContext, object string) error {
	err := ioctx.Delete(object)
	if err != nil {
		if errors.Is(err, rados.ErrNotFound) {
			w.WriteHeader(http.StatusOK)
			return nil
		}
		return fmt.Errorf("delete object %s: %w", object, err)
	}

	w.WriteHeader(http.StatusOK)
	return nil
}

func (h *Handler) listBlobs(w http.ResponseWriter, r *http.Request) {
	verboseLog.Printf("GET %v\n", r.URL)

	blobType := r.PathValue("type")
	if !isValidBlobType(blobType) {
		http.NotFound(w, r)
		return
	}

	ioctx, err := h.openIOContext()
	if err != nil {
		if errors.Is(err, rados.ErrNotFound) {
			http.NotFound(w, r)
			return
		}
		log.Printf("failed to open IO context: %v\n", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}
	defer ioctx.Destroy()

	if err := listBlobsInContext(w, r, ioctx, blobType); err != nil {
		log.Printf("failed to list %s: %v\n", blobType, err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
	}
}

func (h *Handler) checkBlob(w http.ResponseWriter, r *http.Request) {
	verboseLog.Printf("%v %v\n", r.Method, r.URL)

	blobType := r.PathValue("type")
	if !isValidBlobType(blobType) {
		http.NotFound(w, r)
		return
	}

	blobID := r.PathValue("id")
	if !hexBlobIDRegex.MatchString(blobID) {
		http.NotFound(w, r)
		return
	}

	ioctx, err := h.openIOContext()
	if err != nil {
		if errors.Is(err, rados.ErrNotFound) {
			http.NotFound(w, r)
			return
		}
		log.Printf("failed to open IO context: %v\n", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}
	defer ioctx.Destroy()

	objectName := blobType + "/" + blobID

	if err := serveRadosObjectWithRequest(w, r, ioctx, objectName, true); err != nil {
		h.handleRadosError(w, r, blobID, err)
	}
}

func (h *Handler) getBlob(w http.ResponseWriter, r *http.Request) {
	verboseLog.Printf("%v %v\n", r.Method, r.URL)

	blobType := r.PathValue("type")
	if !isValidBlobType(blobType) {
		http.NotFound(w, r)
		return
	}

	blobID := r.PathValue("id")
	if !hexBlobIDRegex.MatchString(blobID) {
		http.NotFound(w, r)
		return
	}

	ioctx, err := h.openIOContext()
	if err != nil {
		if errors.Is(err, rados.ErrNotFound) {
			http.NotFound(w, r)
			return
		}
		log.Printf("failed to open IO context: %v\n", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}
	defer ioctx.Destroy()

	objectName := blobType + "/" + blobID

	if err := serveRadosObjectWithRequest(w, r, ioctx, objectName, false); err != nil {
		h.handleRadosError(w, r, blobID, err)
	}
}

func (h *Handler) saveBlob(w http.ResponseWriter, r *http.Request) {
	verboseLog.Printf("%v %v\n", r.Method, r.URL)

	blobType := r.PathValue("type")
	if !isValidBlobType(blobType) {
		http.NotFound(w, r)
		return
	}

	blobID := r.PathValue("id")
	if !hexBlobIDRegex.MatchString(blobID) {
		http.NotFound(w, r)
		return
	}

	ioctx, err := h.openIOContext()
	if err != nil {
		if errors.Is(err, rados.ErrNotFound) {
			http.NotFound(w, r)
			return
		}
		log.Printf("failed to open IO context: %v\n", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}
	defer ioctx.Destroy()

	objectName := blobType + "/" + blobID

	if err := createRadosObject(w, r, ioctx, objectName, blobID); err != nil {
		h.handleRadosError(w, r, blobID, err)
	}
}

func (h *Handler) deleteBlob(w http.ResponseWriter, r *http.Request) {
	verboseLog.Printf("%v %v\n", r.Method, r.URL)

	blobType := r.PathValue("type")
	if !isValidBlobType(blobType) {
		http.NotFound(w, r)
		return
	}

	blobID := r.PathValue("id")
	if !hexBlobIDRegex.MatchString(blobID) {
		http.NotFound(w, r)
		return
	}

	if h.appendOnly && blobType != "locks" {
		verboseLog.Printf("delete blocked in append-only mode for type %s\n", blobType)
		http.Error(w, "delete not allowed in append-only mode", http.StatusForbidden)
		return
	}

	ioctx, err := h.openIOContext()
	if err != nil {
		if errors.Is(err, rados.ErrNotFound) {
			http.NotFound(w, r)
			return
		}
		log.Printf("failed to open IO context: %v\n", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}
	defer ioctx.Destroy()

	objectName := blobType + "/" + blobID

	if err := deleteRadosObject(w, ioctx, objectName); err != nil {
		h.handleRadosError(w, r, blobID, err)
	}
}

type blobInfo struct {
	Name string `json:"name"`
	Size uint64 `json:"size"`
}

func prefersBlobListV2(r *http.Request) bool {
	for _, value := range r.Header.Values("Accept") {
		for _, mediaRange := range strings.Split(value, ",") {
			mediaRange = strings.TrimSpace(mediaRange)
			if mediaRange == "" {
				continue
			}
			mediaType, params, err := mime.ParseMediaType(mediaRange)
			if err != nil {
				continue
			}
			if mediaType != "application/vnd.x.restic.rest.v2" {
				continue
			}
			if qValue, ok := params["q"]; ok {
				q, err := strconv.ParseFloat(qValue, 64)
				if err == nil && q == 0 {
					continue
				}
			}
			return true
		}
	}
	return false
}

func listBlobsInContext(w http.ResponseWriter, r *http.Request, ioctx *rados.IOContext, blobType string) error {
	iter, err := ioctx.Iter()
	if err != nil {
		return fmt.Errorf("create iterator: %w", err)
	}
	defer iter.Close()

	useV2 := prefersBlobListV2(r)
	prefix := blobType + "/"

	var blobNames []string
	var blobInfos []blobInfo

	for iter.Next() {
		objectName := iter.Value()
		if objectName != "" && strings.HasPrefix(objectName, prefix) {
			blobID := strings.TrimPrefix(objectName, prefix)
			if useV2 {
				stat, err := ioctx.Stat(objectName)
				if err != nil {
					return fmt.Errorf("stat %s: %w", objectName, err)
				}
				blobInfos = append(blobInfos, blobInfo{
					Name: blobID,
					Size: stat.Size,
				})
			} else {
				blobNames = append(blobNames, blobID)
			}
		}
	}

	if err := iter.Err(); err != nil {
		return fmt.Errorf("iterate objects: %w", err)
	}

	var data []byte
	if useV2 {
		data, err = json.Marshal(blobInfos)
		if err != nil {
			return fmt.Errorf("marshal JSON: %w", err)
		}
		w.Header().Set("Content-Type", "application/vnd.x.restic.rest.v2")
	} else {
		data, err = json.Marshal(blobNames)
		if err != nil {
			return fmt.Errorf("marshal JSON: %w", err)
		}
		w.Header().Set("Content-Type", "application/vnd.x.restic.rest.v1")
	}

	w.WriteHeader(http.StatusOK)
	_, err = w.Write(data)
	return err
}
