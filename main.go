package main

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/http"
	"os"
	"os/signal"
	"regexp"
	"strconv"
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

		ioctx, err := conn.OpenIOContext(poolName)
		if err != nil {
			if errors.Is(err, rados.ErrNotFound) {
				http.NotFound(w, r)
				return
			}
			fmt.Fprintf(os.Stderr, "failed to open IO context: %v\n", err)
			http.Error(w, "internal server error", http.StatusInternalServerError)
			return
		}
		defer ioctx.Destroy()

		switch r.Method {
		case "HEAD":
			if err := serveRadosObject(w, ioctx, "config", true); err != nil {
				handleRadosError(w, r, "config", err)
			}
		case "GET":
			if err := serveRadosObject(w, ioctx, "config", false); err != nil {
				handleRadosError(w, r, "config", err)
			}
		default:
			http.NotFound(w, r)
		}
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
			ioctx, err := conn.OpenIOContext(poolName)
			if err != nil {
				fmt.Fprintf(os.Stderr, "failed to open IO context: %v\n", err)
				http.Error(w, "internal server error", http.StatusInternalServerError)
				return
			}
			defer ioctx.Destroy()

			objectTypes := []string{"data", "index", "keys", "locks", "snapshots"}
			for _, objType := range objectTypes {
				err := ioctx.Create(objType+"/.keep", rados.CreateExclusive)
				if err != nil && err != rados.ErrObjectExists {
					fmt.Fprintf(os.Stderr, "failed to create object type directory %s: %v\n", objType, err)
					http.Error(w, "internal server error", http.StatusInternalServerError)
					return
				}
			}

			for i := 0; i < 256; i++ {
				dirName := fmt.Sprintf("data/%02x/.keep", i)
				err := ioctx.Create(dirName, rados.CreateExclusive)
				if err != nil && err != rados.ErrObjectExists {
					fmt.Fprintf(os.Stderr, "failed to create data subdirectory %02x: %v\n", i, err)
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

const radosReadChunkSize = 32 * 1024

func serveRadosObject(w http.ResponseWriter, ioctx *rados.IOContext, object string, head bool) error {
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

	w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
	w.WriteHeader(http.StatusOK)

	if head || size == 0 {
		return nil
	}

	buffer := make([]byte, radosReadChunkSize)
	remaining := size
	var offset int64

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

func handleRadosError(w http.ResponseWriter, r *http.Request, object string, err error) {
	switch {
	case errors.Is(err, errObjectNotFound):
		http.NotFound(w, r)
	default:
		fmt.Fprintf(os.Stderr, "failed to serve %s: %v\n", object, err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
	}
}
