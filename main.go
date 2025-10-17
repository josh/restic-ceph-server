package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
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

var errObjectNotFound = errors.New("object not found")
var errObjectExists = errors.New("object exists")
var errHashMismatch = errors.New("hash mismatch")
var errWriteVerification = errors.New("write verification failed")

func initLogger() error {
	log.SetOutput(os.Stderr)
	log.SetFlags(0)

	logFilePath := os.Getenv("__RESTIC_CEPH_SERVER_LOG_FILE")
	if logFilePath != "" {
		file, err := os.OpenFile(logFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			return fmt.Errorf("failed to open log file %s: %w", logFilePath, err)
		}
		log.SetOutput(file)
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
	if err := initLogger(); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}

	config, err := parseConfig()
	if err != nil {
		log.Printf("%v\n", err)
		os.Exit(1)
	}

	handler := http.NewServeMux()

	handler.HandleFunc("/config", func(w http.ResponseWriter, r *http.Request) {
		log.Printf("%v %v\n", r.Method, r.URL)

		poolName := os.Getenv("CEPH_POOL")
		if poolName == "" {
			log.Printf("CEPH_POOL environment variable not set\n")
			http.NotFound(w, r)
			return
		}

		conn, err := getCephConnection()
		if err != nil {
			log.Printf("failed to get Ceph connection: %v\n", err)
			http.Error(w, "internal server error", http.StatusInternalServerError)
			return
		}

		ioctx, err := conn.OpenIOContext(poolName)
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

		switch r.Method {
		case "HEAD":
			if err := serveRadosObjectWithRequest(w, r, ioctx, "config", true); err != nil {
				handleRadosError(w, r, "config", err)
			}
		case "GET":
			if err := serveRadosObjectWithRequest(w, r, ioctx, "config", false); err != nil {
				handleRadosError(w, r, "config", err)
			}
		case "POST":
			if err := createRadosObject(w, r, ioctx, "config"); err != nil {
				handleRadosError(w, r, "config", err)
			}
		case "DELETE":
			if err := deleteRadosObject(w, ioctx, "config"); err != nil {
				handleRadosError(w, r, "config", err)
			}
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})

	blobTypes := []string{"keys", "locks", "snapshots", "data", "index"}
	for _, blobType := range blobTypes {
		blobType := blobType
		handler.HandleFunc("/"+blobType+"/", createBlobHandler(blobType))
	}

	handler.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		fileTestRegex := regexp.MustCompile(`^/file-\d+$`)
		if r.Method == "GET" && fileTestRegex.MatchString(r.URL.Path) {
			http.NotFound(w, r)
			return
		}

		log.Printf("%v %v\n", r.Method, r.URL)

		poolName := os.Getenv("CEPH_POOL")
		if poolName == "" {
			log.Printf("CEPH_POOL environment variable not set\n")
			http.NotFound(w, r)
			return
		}

		conn, err := getCephConnection()
		if err != nil {
			log.Printf("failed to get Ceph connection: %v\n", err)
			http.Error(w, "internal server error", http.StatusInternalServerError)
			return
		}

		_, err = conn.GetPoolByName(poolName)
		if err != nil {
			log.Printf("pool check failed: pool '%s' does not exist: %v\n", poolName, err)
			http.NotFound(w, r)
			return
		}

		if r.Method == "POST" && r.URL.Path == "/" && r.URL.Query().Get("create") == "true" {
			ioctx, err := conn.OpenIOContext(poolName)
			if err != nil {
				log.Printf("failed to open IO context: %v\n", err)
				http.Error(w, "internal server error", http.StatusInternalServerError)
				return
			}
			defer ioctx.Destroy()

			objectTypes := []string{"data", "index", "keys", "locks", "snapshots"}
			for _, objType := range objectTypes {
				err := ioctx.Create(objType+"/.keep", rados.CreateExclusive)
				if err != nil && err != rados.ErrObjectExists {
					log.Printf("failed to create object type directory %s: %v\n", objType, err)
					http.Error(w, "internal server error", http.StatusInternalServerError)
					return
				}
			}

			for i := 0; i < 256; i++ {
				dirName := fmt.Sprintf("data/%02x/.keep", i)
				err := ioctx.Create(dirName, rados.CreateExclusive)
				if err != nil && err != rados.ErrObjectExists {
					log.Printf("failed to create data subdirectory %02x: %v\n", i, err)
					http.Error(w, "internal server error", http.StatusInternalServerError)
					return
				}
			}

			w.WriteHeader(http.StatusOK)
			return
		}

		http.NotFound(w, r)
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
		log.Printf("Server terminated due to deadline\n")
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

const radosReadChunkSize = 32 * 1024

func expectedHash(object string) ([32]byte, error) {
	if object == "config" {
		return [32]byte{}, nil
	}
	parts := strings.Split(object, "/")
	hashStr := parts[len(parts)-1]

	hashBytes, err := hex.DecodeString(hashStr)
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

func createRadosObject(w http.ResponseWriter, r *http.Request, ioctx *rados.IOContext, object string) error {
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

	expected, err := expectedHash(object)
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

	writeOp := rados.CreateWriteOp()
	defer writeOp.Release()

	writeOp.Create(rados.CreateExclusive)
	writeOp.SetAllocationHint(uint64(len(data)), uint64(len(data)), rados.AllocHintIncompressible|rados.AllocHintImmutable|rados.AllocHintLonglived)
	writeOp.WriteFull(data)

	err = writeOp.Operate(ioctx, object, rados.OperationNoFlag)
	if err != nil {
		if errors.Is(err, rados.ErrObjectExists) {
			return errObjectExists
		}
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

func handleRadosError(w http.ResponseWriter, r *http.Request, object string, err error) {
	switch {
	case errors.Is(err, errObjectNotFound):
		http.NotFound(w, r)
	case errors.Is(err, errObjectExists):
		http.Error(w, "object already exists", http.StatusForbidden)
	case errors.Is(err, errHashMismatch):
		http.Error(w, "hash mismatch", http.StatusBadRequest)
	case errors.Is(err, errWriteVerification):
		http.Error(w, "write verification failed", http.StatusInternalServerError)
	default:
		log.Printf("failed to serve %s: %v\n", object, err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
	}
}

func createBlobHandler(blobType string) http.HandlerFunc {
	blobRegex := regexp.MustCompile(`^/` + blobType + `/([0-9a-fA-F]+)$`)

	return func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/"+blobType+"/" && r.Method == "GET" {
			log.Printf("GET %v\n", r.URL)

			poolName := os.Getenv("CEPH_POOL")
			if poolName == "" {
				log.Printf("CEPH_POOL environment variable not set\n")
				http.NotFound(w, r)
				return
			}

			conn, err := getCephConnection()
			if err != nil {
				log.Printf("failed to get Ceph connection: %v\n", err)
				http.Error(w, "internal server error", http.StatusInternalServerError)
				return
			}

			ioctx, err := conn.OpenIOContext(poolName)
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

			if err := listBlobs(w, r, ioctx, blobType); err != nil {
				log.Printf("failed to list %s: %v\n", blobType, err)
				http.Error(w, "internal server error", http.StatusInternalServerError)
			}
			return
		}

		matches := blobRegex.FindStringSubmatch(r.URL.Path)
		if matches == nil {
			http.NotFound(w, r)
			return
		}

		log.Printf("%v %v\n", r.Method, r.URL)

		blobID := matches[1]
		objectName := blobType + "/" + blobID

		poolName := os.Getenv("CEPH_POOL")
		if poolName == "" {
			log.Printf("CEPH_POOL environment variable not set\n")
			http.NotFound(w, r)
			return
		}

		conn, err := getCephConnection()
		if err != nil {
			log.Printf("failed to get Ceph connection: %v\n", err)
			http.Error(w, "internal server error", http.StatusInternalServerError)
			return
		}

		ioctx, err := conn.OpenIOContext(poolName)
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

		switch r.Method {
		case "HEAD":
			if err := serveRadosObjectWithRequest(w, r, ioctx, objectName, true); err != nil {
				handleRadosError(w, r, objectName, err)
			}
		case "GET":
			if err := serveRadosObjectWithRequest(w, r, ioctx, objectName, false); err != nil {
				handleRadosError(w, r, objectName, err)
			}
		case "POST":
			if err := createRadosObject(w, r, ioctx, objectName); err != nil {
				handleRadosError(w, r, objectName, err)
			}
		case "DELETE":
			if err := deleteRadosObject(w, ioctx, objectName); err != nil {
				handleRadosError(w, r, objectName, err)
			}
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	}
}

type blobInfo struct {
	Name string `json:"name"`
	Size uint64 `json:"size"`
}

func listBlobs(w http.ResponseWriter, r *http.Request, ioctx *rados.IOContext, blobType string) error {
	iter, err := ioctx.Iter()
	if err != nil {
		return fmt.Errorf("create iterator: %w", err)
	}
	defer iter.Close()

	acceptHeader := r.Header.Get("Accept")
	useV2 := strings.Contains(acceptHeader, "application/vnd.x.restic.rest.v2")

	prefix := blobType + "/"
	var blobNames []string
	var blobInfos []blobInfo

	for iter.Next() {
		objName := iter.Value()
		if strings.HasPrefix(objName, prefix) {
			blobID := strings.TrimPrefix(objName, prefix)
			if blobID != "" && !strings.HasSuffix(blobID, ".keep") {
				if useV2 {
					stat, err := ioctx.Stat(objName)
					if err != nil {
						return fmt.Errorf("stat %s: %w", objName, err)
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
