package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"mime"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/ceph/go-ceph/rados"
	"github.com/ceph/go-ceph/rados/striper"
)

type Handler struct {
	connMgr         *ConnectionManager
	appendOnly      bool
	logger          *slog.Logger
	maxObjectSize   int64
	maxObjectSizeMu sync.RWMutex
	striperLayout   striper.Layout
	striperEnabled  bool
}

type responseWriter struct {
	http.ResponseWriter
	statusCode    int
	bytesWritten  int64
	headerWritten bool
}

func (rw *responseWriter) WriteHeader(code int) {
	if rw.headerWritten {
		return
	}
	rw.statusCode = code
	rw.headerWritten = true
	rw.ResponseWriter.WriteHeader(code)
}

func (rw *responseWriter) Write(b []byte) (int, error) {
	if !rw.headerWritten {
		rw.statusCode = http.StatusOK
		rw.headerWritten = true
	}
	n, err := rw.ResponseWriter.Write(b)
	rw.bytesWritten += int64(n)
	return n, err
}

func (h *Handler) logRequest(method, path string, status int, duration time.Duration, reqBytes, respBytes int64) {
	h.logger.Debug("request",
		"method", method,
		"path", path,
		"status", status,
		"duration", duration.Round(time.Millisecond).String(),
		"req_bytes", reqBytes,
		"resp_bytes", respBytes,
	)
}

func (h *Handler) openIOContext(w http.ResponseWriter, r *http.Request) (*rados.IOContext, bool) {
	ioctx, err := h.connMgr.GetIOContext()
	if err != nil {
		if errors.Is(err, errConnectionUnavailable) {
			http.Error(w, "ceph cluster unavailable", http.StatusServiceUnavailable)
		} else if errors.Is(err, rados.ErrNotFound) {
			http.NotFound(w, r)
		} else {
			h.logger.Error("failed to open IO context", "error", err)
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
	case errors.Is(err, errConnectionUnavailable):
		http.Error(w, "ceph cluster unavailable", http.StatusServiceUnavailable)
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
		h.logger.Error("failed to serve object", "object", object, "error", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
	}
}

func (h *Handler) checkConfig(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	rw := &responseWriter{ResponseWriter: w, statusCode: http.StatusOK}
	defer func() {
		h.logRequest(r.Method, r.URL.Path, rw.statusCode, time.Since(start), r.ContentLength, rw.bytesWritten)
	}()

	ioctx, ok := h.openIOContext(rw, r)
	if !ok {
		return
	}
	defer ioctx.Destroy()

	if err := h.serveRadosObjectWithRequest(rw, r, ioctx, "config", true); err != nil {
		h.handleRadosError(rw, r, "config", err)
	}
}

func (h *Handler) getConfig(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	rw := &responseWriter{ResponseWriter: w, statusCode: http.StatusOK}
	defer func() {
		h.logRequest(r.Method, r.URL.Path, rw.statusCode, time.Since(start), r.ContentLength, rw.bytesWritten)
	}()

	ioctx, ok := h.openIOContext(rw, r)
	if !ok {
		return
	}
	defer ioctx.Destroy()

	if err := h.serveRadosObjectWithRequest(rw, r, ioctx, "config", false); err != nil {
		h.handleRadosError(rw, r, "config", err)
	}
}

func (h *Handler) saveConfig(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	rw := &responseWriter{ResponseWriter: w, statusCode: http.StatusOK}
	defer func() {
		h.logRequest(r.Method, r.URL.Path, rw.statusCode, time.Since(start), r.ContentLength, rw.bytesWritten)
	}()

	ioctx, ok := h.openIOContext(rw, r)
	if !ok {
		return
	}
	defer ioctx.Destroy()

	if err := h.createRadosObject(rw, r, ioctx, "config", "config"); err != nil {
		h.handleRadosError(rw, r, "config", err)
	}
}

func (h *Handler) deleteConfig(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	rw := &responseWriter{ResponseWriter: w, statusCode: http.StatusOK}
	defer func() {
		h.logRequest(r.Method, r.URL.Path, rw.statusCode, time.Since(start), r.ContentLength, rw.bytesWritten)
	}()

	if h.appendOnly {
		h.logger.Debug("delete blocked in append-only mode", "object", "config")
		http.Error(rw, "delete not allowed in append-only mode", http.StatusForbidden)
		return
	}

	ioctx, ok := h.openIOContext(rw, r)
	if !ok {
		return
	}
	defer ioctx.Destroy()

	if err := h.deleteRadosObject(rw, ioctx, "config"); err != nil {
		h.handleRadosError(rw, r, "config", err)
	}
}

func (h *Handler) createRepo(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	rw := &responseWriter{ResponseWriter: w, statusCode: http.StatusOK}
	defer func() {
		h.logRequest(r.Method, r.URL.Path, rw.statusCode, time.Since(start), r.ContentLength, rw.bytesWritten)
	}()

	conn, err := h.connMgr.GetConnection()
	if err != nil {
		if errors.Is(err, errConnectionUnavailable) {
			http.Error(rw, "ceph cluster unavailable", http.StatusServiceUnavailable)
		} else {
			h.logger.Error("failed to get connection", "error", err)
			http.Error(rw, "internal server error", http.StatusInternalServerError)
		}
		return
	}

	_, err = conn.GetPoolByName(h.connMgr.config.PoolName)
	if err != nil {
		h.logger.Error("pool check failed", "pool", h.connMgr.config.PoolName, "error", err)
		http.NotFound(rw, r)
		return
	}

	createParam := r.URL.Query().Get("create")
	if createParam == "" {
		http.Error(rw, "missing required query parameter: create", http.StatusBadRequest)
		return
	}
	if createParam != "true" {
		http.Error(rw, "invalid value for create parameter: must be 'true'", http.StatusBadRequest)
		return
	}

	ioctx, ok := h.openIOContext(rw, r)
	if !ok {
		return
	}
	defer ioctx.Destroy()

	rw.WriteHeader(http.StatusOK)
}

func (h *Handler) listBlobs(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	rw := &responseWriter{ResponseWriter: w, statusCode: http.StatusOK}
	defer func() {
		h.logRequest(r.Method, r.URL.Path, rw.statusCode, time.Since(start), r.ContentLength, rw.bytesWritten)
	}()

	blobType := r.PathValue("type")
	if !isValidBlobType(blobType) {
		http.NotFound(rw, r)
		return
	}

	ioctx, ok := h.openIOContext(rw, r)
	if !ok {
		return
	}
	defer ioctx.Destroy()

	if err := h.listBlobsInContext(rw, r, ioctx, blobType); err != nil {
		h.logger.Error("failed to list blobs", "type", blobType, "error", err)
		http.Error(rw, "internal server error", http.StatusInternalServerError)
	}
}

func (h *Handler) checkBlob(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	rw := &responseWriter{ResponseWriter: w, statusCode: http.StatusOK}
	defer func() {
		h.logRequest(r.Method, r.URL.Path, rw.statusCode, time.Since(start), r.ContentLength, rw.bytesWritten)
	}()

	blobType := r.PathValue("type")
	if !isValidBlobType(blobType) {
		http.NotFound(rw, r)
		return
	}

	blobID := r.PathValue("id")
	if !hexBlobIDRegex.MatchString(blobID) {
		http.NotFound(rw, r)
		return
	}

	ioctx, ok := h.openIOContext(rw, r)
	if !ok {
		return
	}
	defer ioctx.Destroy()

	objectName := blobType + "/" + blobID

	if err := h.serveRadosObjectWithRequest(rw, r, ioctx, objectName, true); err != nil {
		h.handleRadosError(rw, r, blobID, err)
	}
}

func (h *Handler) getBlob(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	rw := &responseWriter{ResponseWriter: w, statusCode: http.StatusOK}
	defer func() {
		h.logRequest(r.Method, r.URL.Path, rw.statusCode, time.Since(start), r.ContentLength, rw.bytesWritten)
	}()

	blobType := r.PathValue("type")
	if !isValidBlobType(blobType) {
		http.NotFound(rw, r)
		return
	}

	blobID := r.PathValue("id")
	if !hexBlobIDRegex.MatchString(blobID) {
		http.NotFound(rw, r)
		return
	}

	ioctx, ok := h.openIOContext(rw, r)
	if !ok {
		return
	}
	defer ioctx.Destroy()

	objectName := blobType + "/" + blobID

	if err := h.serveRadosObjectWithRequest(rw, r, ioctx, objectName, false); err != nil {
		h.handleRadosError(rw, r, blobID, err)
	}
}

func (h *Handler) saveBlob(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	rw := &responseWriter{ResponseWriter: w, statusCode: http.StatusOK}
	defer func() {
		h.logRequest(r.Method, r.URL.Path, rw.statusCode, time.Since(start), r.ContentLength, rw.bytesWritten)
	}()

	blobType := r.PathValue("type")
	if !isValidBlobType(blobType) {
		http.NotFound(rw, r)
		return
	}

	blobID := r.PathValue("id")
	if !hexBlobIDRegex.MatchString(blobID) {
		http.NotFound(rw, r)
		return
	}

	ioctx, ok := h.openIOContext(rw, r)
	if !ok {
		return
	}
	defer ioctx.Destroy()

	objectName := blobType + "/" + blobID

	if err := h.createRadosObject(rw, r, ioctx, objectName, blobID); err != nil {
		h.handleRadosError(rw, r, blobID, err)
	}
}

func (h *Handler) deleteBlob(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	rw := &responseWriter{ResponseWriter: w, statusCode: http.StatusOK}
	defer func() {
		h.logRequest(r.Method, r.URL.Path, rw.statusCode, time.Since(start), r.ContentLength, rw.bytesWritten)
	}()

	blobType := r.PathValue("type")
	if !isValidBlobType(blobType) {
		http.NotFound(rw, r)
		return
	}

	blobID := r.PathValue("id")
	if !hexBlobIDRegex.MatchString(blobID) {
		http.NotFound(rw, r)
		return
	}

	if h.appendOnly && blobType != "locks" {
		h.logger.Debug("delete blocked in append-only mode", "type", blobType)
		http.Error(rw, "delete not allowed in append-only mode", http.StatusForbidden)
		return
	}

	ioctx, ok := h.openIOContext(rw, r)
	if !ok {
		return
	}
	defer ioctx.Destroy()

	objectName := blobType + "/" + blobID

	if err := h.deleteRadosObject(rw, ioctx, objectName); err != nil {
		h.handleRadosError(rw, r, blobID, err)
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

func (h *Handler) listBlobsInContext(w http.ResponseWriter, r *http.Request, ioctx *rados.IOContext, blobType string) error {
	iter, err := ioctx.Iter()
	if err != nil {
		return fmt.Errorf("create iterator: %w", err)
	}
	defer iter.Close()

	useV2 := prefersBlobListV2(r)
	prefix := blobType + "/"

	blobNames := []string{}
	blobInfos := []blobInfo{}
	seenBlobs := make(map[string]bool)

	for iter.Next() {
		objectName := iter.Value()
		if objectName != "" && strings.HasPrefix(objectName, prefix) {
			blobID := strings.TrimPrefix(objectName, prefix)

			if stripedBlobIDRegex.MatchString(blobID) {
				blobID = blobID[:len(blobID)-17]
			}

			if !hexBlobIDRegex.MatchString(blobID) {
				continue
			}

			if seenBlobs[blobID] {
				continue
			}
			seenBlobs[blobID] = true

			baseObjectName := prefix + blobID

			if useV2 {
				stat, err := ioctx.Stat(baseObjectName)
				if err != nil {
					if errors.Is(err, rados.ErrNotFound) {
						isStriped, stripErr := isStripedObject(ioctx, baseObjectName)
						if stripErr != nil {
							return fmt.Errorf("check if striped: %w", stripErr)
						}
						if isStriped {
							s, err := h.createStriperWithDefaults(ioctx)
							if err != nil {
								return fmt.Errorf("create striper for stat: %w", err)
							}
							defer s.Destroy()

							stripStat, err := s.Stat(baseObjectName)
							if err != nil {
								return fmt.Errorf("stat striped object %s: %w", baseObjectName, err)
							}
							blobInfos = append(blobInfos, blobInfo{
								Name: blobID,
								Size: stripStat.Size,
							})
							continue
						}
					}
					return fmt.Errorf("stat %s: %w", baseObjectName, err)
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

func isStripedObject(ioctx *rados.IOContext, object string) (bool, error) {
	firstStripeName := object + ".0000000000000000"
	_, err := ioctx.Stat(firstStripeName)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, rados.ErrNotFound) {
		return false, nil
	}
	return false, fmt.Errorf("stat first stripe %s: %w", firstStripeName, err)
}

type httpRange struct {
	start  int64
	end    int64
	status int
}

func parseRange(r *http.Request, size int64) (*httpRange, error) {
	if r == nil {
		return &httpRange{start: 0, end: size - 1, status: http.StatusOK}, nil
	}

	rangeHeader := r.Header.Get("Range")
	if rangeHeader == "" {
		return &httpRange{start: 0, end: size - 1, status: http.StatusOK}, nil
	}

	if !strings.HasPrefix(rangeHeader, "bytes=") {
		return nil, fmt.Errorf("unsupported range unit in: %s", rangeHeader)
	}

	rangeSpec := strings.TrimPrefix(rangeHeader, "bytes=")

	if strings.Contains(rangeSpec, ",") {
		return nil, fmt.Errorf("multiple ranges not supported: %s", rangeHeader)
	}

	parts := strings.Split(rangeSpec, "-")
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid range format: %s", rangeHeader)
	}

	if parts[0] == "" && parts[1] == "" {
		return nil, fmt.Errorf("empty range spec: %s", rangeHeader)
	}

	var start, end int64

	if parts[0] == "" {
		suffixLength, err := strconv.ParseInt(parts[1], 10, 64)
		if err != nil || suffixLength < 0 {
			return nil, fmt.Errorf("invalid suffix length in range: %s", rangeHeader)
		}
		if suffixLength >= size {
			start = 0
		} else {
			start = size - suffixLength
		}
		end = size - 1
	} else {
		rangeStart, err := strconv.ParseInt(parts[0], 10, 64)
		if err != nil || rangeStart < 0 {
			return nil, fmt.Errorf("invalid range start: %w", err)
		}

		if rangeStart >= size {
			return nil, fmt.Errorf("range start %d out of bounds for size %d", rangeStart, size)
		}

		start = rangeStart

		if parts[1] != "" {
			rangeEnd, err := strconv.ParseInt(parts[1], 10, 64)
			if err != nil || rangeEnd < 0 {
				return nil, fmt.Errorf("invalid range end: %w", err)
			}
			if rangeEnd >= size {
				rangeEnd = size - 1
			}
			end = rangeEnd
		} else {
			end = size - 1
		}

		if start > end {
			return nil, fmt.Errorf("range start %d greater than end %d", start, end)
		}
	}

	return &httpRange{start: start, end: end, status: http.StatusPartialContent}, nil
}

func serveRegularObject(w http.ResponseWriter, r *http.Request, ioctx *rados.IOContext, object string, stat rados.ObjectStat, head bool) error {
	if stat.Size > uint64(math.MaxInt64) {
		return fmt.Errorf("object %s size exceeds max int64: %d", object, stat.Size)
	}

	size := int64(stat.Size)

	rng, err := parseRange(r, size)
	if err != nil {
		http.Error(w, err.Error(), http.StatusRequestedRangeNotSatisfiable)
		return err
	}

	if rng.status == http.StatusPartialContent {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", rng.start, rng.end, size))
	}

	contentLength := rng.end - rng.start + 1
	w.Header().Set("Content-Length", strconv.FormatInt(contentLength, 10))
	w.WriteHeader(rng.status)

	if head || contentLength == 0 {
		return nil
	}

	buffer := make([]byte, radosReadChunkSize)
	remaining := contentLength
	offset := rng.start

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

func (h *Handler) serveStripedObject(w http.ResponseWriter, r *http.Request, ioctx *rados.IOContext, object string, head bool) error {
	s, err := h.createStriperWithDefaults(ioctx)
	if err != nil {
		return fmt.Errorf("create striper: %w", err)
	}
	defer s.Destroy()

	stat, err := s.Stat(object)
	if err != nil {
		return fmt.Errorf("stat striped object %s: %w", object, err)
	}

	if stat.Size > uint64(math.MaxInt64) {
		return fmt.Errorf("striped object %s size exceeds max int64: %d", object, stat.Size)
	}

	size := int64(stat.Size)

	rng, err := parseRange(r, size)
	if err != nil {
		http.Error(w, err.Error(), http.StatusRequestedRangeNotSatisfiable)
		return err
	}

	if rng.status == http.StatusPartialContent {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", rng.start, rng.end, size))
	}

	contentLength := rng.end - rng.start + 1
	w.Header().Set("Content-Length", strconv.FormatInt(contentLength, 10))
	w.WriteHeader(rng.status)

	if head || contentLength == 0 {
		return nil
	}

	buffer := make([]byte, radosReadChunkSize)
	remaining := contentLength
	offset := rng.start

	for remaining > 0 {
		chunkSize := len(buffer)
		if remaining < int64(chunkSize) {
			chunkSize = int(remaining)
		}

		n, err := s.Read(object, buffer[:chunkSize], uint64(offset))
		if err != nil {
			return fmt.Errorf("read striped object %s: %w", object, err)
		}
		if n == 0 {
			return fmt.Errorf("short read on striped object %s", object)
		}

		if _, err := w.Write(buffer[:n]); err != nil {
			return fmt.Errorf("write response: %w", err)
		}

		offset += int64(n)
		remaining -= int64(n)
	}

	return nil
}

func (h *Handler) serveRadosObjectWithRequest(w http.ResponseWriter, r *http.Request, ioctx *rados.IOContext, object string, head bool) error {
	stat, err := ioctx.Stat(object)
	if err != nil {
		if errors.Is(err, rados.ErrNotFound) {
			isStriped, stripErr := isStripedObject(ioctx, object)
			if stripErr != nil {
				return fmt.Errorf("check if striped object: %w", stripErr)
			}
			if isStriped {
				h.logger.Debug("serving striped object", "object", object)
				return h.serveStripedObject(w, r, ioctx, object, head)
			}
			return errObjectNotFound
		}
		return fmt.Errorf("stat %s: %w", object, err)
	}

	h.logger.Debug("serving regular object", "object", object)
	return serveRegularObject(w, r, ioctx, object, stat, head)
}

func (h *Handler) writeObjectWithRADOS(w http.ResponseWriter, ioctx *rados.IOContext, object string, data []byte, expected [32]byte) error {
	writeOp := rados.CreateWriteOp()
	defer writeOp.Release()

	writeOp.Create(rados.CreateExclusive)
	writeOp.SetAllocationHint(uint64(len(data)), uint64(len(data)), rados.AllocHintIncompressible|rados.AllocHintImmutable|rados.AllocHintLonglived)
	writeOp.WriteFull(data)

	err := writeOp.Operate(ioctx, object, rados.OperationNoFlag)
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
				h.logger.Error("failed to delete object after write verification failure", "object", object, "error", err)
			}
			h.logger.Error("write verification failed", "object", object, "expected", fmt.Sprintf("%x", expected), "got", fmt.Sprintf("%x", actual))
			return errWriteVerification
		}
	}

	w.WriteHeader(http.StatusOK)
	return nil
}

func (h *Handler) writeObjectWithStriper(w http.ResponseWriter, ioctx *rados.IOContext, object string, data []byte, expected [32]byte) error {
	s, err := h.createStriperWithDefaults(ioctx)
	if err != nil {
		return fmt.Errorf("create striper: %w", err)
	}
	defer s.Destroy()

	err = s.WriteFull(object, data)
	if err != nil {
		return fmt.Errorf("write striped object %s: %w", object, err)
	}

	if expected != [32]byte{} {
		readData := make([]byte, len(data))
		_, err = s.Read(object, readData, 0)
		if err != nil {
			return fmt.Errorf("read striped object %s after write: %w", object, err)
		}

		actual := sha256.Sum256(readData)

		if actual != expected {
			if err := s.Remove(object); err != nil {
				h.logger.Error("failed to delete striped object after write verification failure", "object", object, "error", err)
			}
			h.logger.Error("write verification failed for striped object", "object", object, "expected", fmt.Sprintf("%x", expected), "got", fmt.Sprintf("%x", actual))
			return errWriteVerification
		}
	}

	w.WriteHeader(http.StatusOK)
	return nil
}

func (h *Handler) createRadosObject(w http.ResponseWriter, r *http.Request, ioctx *rados.IOContext, object string, hashID string) error {
	maxSize, _, err := h.getBlobOptions()
	if err != nil {
		return fmt.Errorf("failed to get max object size: %w", err)
	}

	if !h.shouldUseStriper(r.ContentLength) && r.ContentLength > 0 && r.ContentLength > maxSize {
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

	if !h.shouldUseStriper(int64(len(data))) && int64(len(data)) > maxSize {
		return errObjectTooLarge
	}

	expected, err := expectedHash(hashID)
	if err != nil {
		return err
	}

	if expected != [32]byte{} {
		actual := sha256.Sum256(data)
		if actual != expected {
			h.logger.Error("input hash mismatch", "object", object, "expected", fmt.Sprintf("%x", expected), "got", fmt.Sprintf("%x", actual))
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

	if h.shouldUseStriper(int64(len(data))) {
		h.logger.Debug("using striper for large object", "object", object, "size", len(data))
		return h.writeObjectWithStriper(w, ioctx, object, data, expected)
	}

	h.logger.Debug("using regular RADOS for object", "object", object, "size", len(data))
	return h.writeObjectWithRADOS(w, ioctx, object, data, expected)
}

func (h *Handler) deleteRadosObject(w http.ResponseWriter, ioctx *rados.IOContext, object string) error {
	err := ioctx.Delete(object)
	if err != nil {
		if errors.Is(err, rados.ErrNotFound) {
			isStriped, stripErr := isStripedObject(ioctx, object)
			if stripErr != nil {
				return fmt.Errorf("check if striped object: %w", stripErr)
			}
			if isStriped {
				s, err := h.createStriperWithDefaults(ioctx)
				if err != nil {
					return fmt.Errorf("create striper for delete: %w", err)
				}
				defer s.Destroy()

				err = s.Remove(object)
				if err != nil {
					return fmt.Errorf("delete striped object %s: %w", object, err)
				}
				w.WriteHeader(http.StatusOK)
				return nil
			}
			w.WriteHeader(http.StatusOK)
			return nil
		}
		return fmt.Errorf("delete object %s: %w", object, err)
	}

	w.WriteHeader(http.StatusOK)
	return nil
}

func (h *Handler) ensureConfigLoaded() error {
	h.maxObjectSizeMu.RLock()
	if h.maxObjectSize != 0 {
		h.maxObjectSizeMu.RUnlock()
		return nil
	}
	h.maxObjectSizeMu.RUnlock()

	h.maxObjectSizeMu.Lock()
	defer h.maxObjectSizeMu.Unlock()

	if h.maxObjectSize != 0 {
		return nil
	}

	conn, err := h.connMgr.GetConnection()
	if err != nil {
		return fmt.Errorf("failed to get connection: %w", err)
	}

	sizeStr, err := conn.GetConfigOption("osd_max_object_size")
	if err != nil {
		return fmt.Errorf("failed to read osd_max_object_size: %w", err)
	}

	size, err := strconv.ParseInt(sizeStr, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid osd_max_object_size value %q: %w", sizeStr, err)
	}

	h.maxObjectSize = size
	if h.striperEnabled {
		h.striperLayout = striper.Layout{
			StripeUnit:  16 * 1024 * 1024,
			StripeCount: 1,
			ObjectSize:  uint(size),
		}
	}
	return nil
}

func (h *Handler) getBlobOptions() (maxObjectSize int64, layout *striper.Layout, err error) {
	if err := h.ensureConfigLoaded(); err != nil {
		return 0, nil, err
	}
	h.maxObjectSizeMu.RLock()
	defer h.maxObjectSizeMu.RUnlock()
	if h.striperEnabled && h.striperLayout.ObjectSize != 0 {
		return h.maxObjectSize, &h.striperLayout, nil
	}
	return h.maxObjectSize, nil, nil
}

func (h *Handler) shouldUseStriper(size int64) bool {
	if !h.striperEnabled {
		return false
	}
	maxSize, _, err := h.getBlobOptions()
	if err != nil {
		h.logger.Warn("failed to get max object size for striper decision", "error", err)
		return false
	}
	return size > maxSize
}

func (h *Handler) createStriperWithDefaults(ioctx *rados.IOContext) (*striper.Striper, error) {
	_, layout, err := h.getBlobOptions()
	if err != nil {
		return nil, fmt.Errorf("get striper layout: %w", err)
	}
	if layout == nil {
		return nil, fmt.Errorf("striper layout not available (striping disabled)")
	}
	return striper.NewWithLayout(ioctx, *layout)
}
