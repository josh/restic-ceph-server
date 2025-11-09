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
	"mime"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"syscall"

	"github.com/ceph/go-ceph/rados"
)

type Handler struct {
	conn              *rados.Conn
	poolName          string
	appendOnly        bool
	maxObjectSize     int64
	maxObjectSizeOnce sync.Once
	maxObjectSizeErr  error
}

func (h *Handler) openIOContext(w http.ResponseWriter, r *http.Request) (*rados.IOContext, bool) {
	ioctx, err := h.conn.OpenIOContext(h.poolName)
	if err != nil {
		if errors.Is(err, rados.ErrNotFound) {
			http.NotFound(w, r)
		} else {
			log.Printf("failed to open IO context: %v\n", err)
			http.Error(w, "internal server error", http.StatusInternalServerError)
		}
		return nil, false
	}
	return ioctx, true
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
	case errors.Is(err, errClientAborted):
		http.Error(w, "client aborted request", http.StatusBadRequest)
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

	ioctx, ok := h.openIOContext(w, r)
	if !ok {
		return
	}
	defer ioctx.Destroy()

	if err := serveRadosObjectWithRequest(w, r, ioctx, "config", true); err != nil {
		h.handleRadosError(w, r, "config", err)
	}
}

func (h *Handler) getConfig(w http.ResponseWriter, r *http.Request) {
	verboseLog.Printf("%v %v\n", r.Method, r.URL)

	ioctx, ok := h.openIOContext(w, r)
	if !ok {
		return
	}
	defer ioctx.Destroy()

	if err := serveRadosObjectWithRequest(w, r, ioctx, "config", false); err != nil {
		h.handleRadosError(w, r, "config", err)
	}
}

func (h *Handler) saveConfig(w http.ResponseWriter, r *http.Request) {
	verboseLog.Printf("%v %v\n", r.Method, r.URL)

	ioctx, ok := h.openIOContext(w, r)
	if !ok {
		return
	}
	defer ioctx.Destroy()

	if err := h.createRadosObject(w, r, ioctx, "config", "config"); err != nil {
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

	ioctx, ok := h.openIOContext(w, r)
	if !ok {
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

	ioctx, ok := h.openIOContext(w, r)
	if !ok {
		return
	}
	defer ioctx.Destroy()

	w.WriteHeader(http.StatusOK)
}

func (h *Handler) listBlobs(w http.ResponseWriter, r *http.Request) {
	verboseLog.Printf("GET %v\n", r.URL)

	blobType := r.PathValue("type")
	if !isValidBlobType(blobType) {
		http.NotFound(w, r)
		return
	}

	ioctx, ok := h.openIOContext(w, r)
	if !ok {
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

	ioctx, ok := h.openIOContext(w, r)
	if !ok {
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

	ioctx, ok := h.openIOContext(w, r)
	if !ok {
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

	ioctx, ok := h.openIOContext(w, r)
	if !ok {
		return
	}
	defer ioctx.Destroy()

	objectName := blobType + "/" + blobID

	if err := h.createRadosObject(w, r, ioctx, objectName, blobID); err != nil {
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

	ioctx, ok := h.openIOContext(w, r)
	if !ok {
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

	blobNames := []string{}
	blobInfos := []blobInfo{}

	for iter.Next() {
		objectName := iter.Value()
		if objectName != "" && strings.HasPrefix(objectName, prefix) {
			blobID := strings.TrimPrefix(objectName, prefix)
			if !hexBlobIDRegex.MatchString(blobID) {
				continue
			}
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

func (h *Handler) setupRoutes(mux *http.ServeMux) {
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

func (h *Handler) createRadosObject(w http.ResponseWriter, r *http.Request, ioctx *rados.IOContext, object string, hashID string) error {
	maxSize, err := h.getMaxObjectSize()
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
			if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, context.Canceled) {
				return errClientAborted
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

func (h *Handler) getMaxObjectSize() (int64, error) {
	h.maxObjectSizeOnce.Do(func() {
		sizeStr, err := h.conn.GetConfigOption("osd_max_object_size")
		if err != nil {
			h.maxObjectSizeErr = fmt.Errorf("failed to read osd_max_object_size: %w", err)
			return
		}

		size, err := strconv.ParseInt(sizeStr, 10, 64)
		if err != nil {
			h.maxObjectSizeErr = fmt.Errorf("invalid osd_max_object_size value %q: %w", sizeStr, err)
			return
		}

		h.maxObjectSize = size
	})
	return h.maxObjectSize, h.maxObjectSizeErr
}
