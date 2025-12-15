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
	"syscall"
	"time"

	"github.com/ceph/go-ceph/rados"
	"github.com/ceph/go-ceph/rados/striper"
)

type Handler struct {
	connMgr        *ConnectionManager
	appendOnly     bool
	logger         *slog.Logger
	striperEnabled bool
}

type HandlerContext struct {
	radosIO       RadosIOContext
	striperIO     RadosIOContext
	maxObjectSize int64
	logger        *slog.Logger
}

func (hctx *HandlerContext) Destroy() {
	if hctx.striperIO != nil {
		hctx.striperIO.Destroy()
	}
	hctx.radosIO.Destroy()
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
	h.logger.Info("request",
		"method", method,
		"path", path,
		"status", status,
		"duration", duration.Round(time.Millisecond).String(),
		"req_bytes", reqBytes,
		"resp_bytes", respBytes,
	)
}

func (h *Handler) openIOContext(w http.ResponseWriter, r *http.Request) (*HandlerContext, bool) {
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

	maxSize, err := h.connMgr.GetMaxObjectSize()
	if err != nil {
		h.logger.Error("failed to get cluster max object size", "error", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return nil, false
	}

	hctx := &HandlerContext{
		radosIO:       &radosIOContextWrapper{ioctx: ioctx},
		maxObjectSize: maxSize,
		logger:        h.logger,
	}

	if h.striperEnabled {
		layout, err := h.connMgr.GetStriperLayout()
		if err != nil {
			h.logger.Error("failed to get striper layout", "error", err)
			http.Error(w, "internal server error", http.StatusInternalServerError)
			return nil, false
		}
		s, err := striper.NewWithLayout(ioctx, layout)
		if err != nil {
			h.logger.Error("failed to create striper instance", "error", err)
			http.Error(w, "internal server error", http.StatusInternalServerError)
			return nil, false
		}
		hctx.striperIO = &striperIOContextWrapper{striper: s}
	}

	return hctx, true
}

func isValidBlobType(blobType string) bool {
	switch blobType {
	case "keys", "locks", "snapshots", "data", "index":
		return true
	default:
		return false
	}
}

func canStripeBlobType(blobType string) bool {
	switch blobType {
	case "snapshots", "data", "index":
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
				h.logger.Error("insufficient storage", "object", object, "error", err)
				http.Error(w, "insufficient storage", http.StatusInsufficientStorage)
				return
			case -int(syscall.EDQUOT):
				h.logger.Error("disk quota exceeded", "object", object, "error", err)
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
	case errors.Is(err, errObjectTooLarge):
		http.Error(w, "object size exceeds cluster limit", http.StatusRequestEntityTooLarge)
	default:
		h.logger.Error("failed to serve object", "object", object, "error", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
	}
}

func (h *Handler) getConfig(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	rw := &responseWriter{ResponseWriter: w, statusCode: http.StatusOK}
	defer func() {
		h.logRequest(r.Method, r.URL.Path, rw.statusCode, time.Since(start), r.ContentLength, rw.bytesWritten)
	}()

	hctx, ok := h.openIOContext(rw, r)
	if !ok {
		return
	}
	defer hctx.Destroy()

	if err := hctx.serveRadosObject(rw, r, "config"); err != nil {
		h.handleRadosError(rw, r, "config", err)
	}
}

func (h *Handler) createConfig(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	rw := &responseWriter{ResponseWriter: w, statusCode: http.StatusOK}
	defer func() {
		h.logRequest(r.Method, r.URL.Path, rw.statusCode, time.Since(start), r.ContentLength, rw.bytesWritten)
	}()

	hctx, ok := h.openIOContext(rw, r)
	if !ok {
		return
	}
	defer hctx.Destroy()

	if err := hctx.createRadosObject(rw, r, "config", "config", false); err != nil {
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

	hctx, ok := h.openIOContext(rw, r)
	if !ok {
		return
	}
	defer hctx.Destroy()

	_, err := hctx.radosIO.Stat("config")
	if errors.Is(err, rados.ErrNotFound) {
		rw.WriteHeader(http.StatusOK)
		return
	}
	if err != nil {
		h.handleRadosError(rw, r, "config", fmt.Errorf("stat object config: %w", err))
		return
	}

	if err := hctx.radosIO.Remove("config"); err != nil {
		h.handleRadosError(rw, r, "config", fmt.Errorf("delete object %s: %w", "config", err))
		return
	}
	rw.WriteHeader(http.StatusOK)
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
		h.logger.Warn("pool check failed", "pool", h.connMgr.config.PoolName, "error", err)
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

	hctx, ok := h.openIOContext(rw, r)
	if !ok {
		return
	}
	defer hctx.Destroy()

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

	hctx, ok := h.openIOContext(rw, r)
	if !ok {
		return
	}
	defer hctx.Destroy()

	iter, err := hctx.radosIO.Iter()
	if err != nil {
		h.logger.Error("failed to list blobs", "type", blobType, "error", fmt.Errorf("create iterator: %w", err))
		http.Error(rw, "internal server error", http.StatusInternalServerError)
		return
	}
	defer iter.Close()

	useV2 := acceptsBlobListV2(r)
	prefix := blobType + "/"

	blobNames := []string{}
	blobInfos := []blobInfo{}

	for iter.Next() {
		objectName := iter.Value()
		if objectName == "" || !strings.HasPrefix(objectName, prefix) {
			continue
		}

		blobID := strings.TrimPrefix(objectName, prefix)

		if stripedBlobIDRegex.MatchString(blobID) && !firstStripedBlobIDRegex.MatchString(blobID) {
			continue
		}

		if firstStripedBlobIDRegex.MatchString(blobID) {
			blobID = blobID[:len(blobID)-stripeSuffixLen]
		}

		if !hexBlobIDRegex.MatchString(blobID) {
			continue
		}

		baseObjectName := prefix + blobID

		if useV2 {
			_, stat, err := hctx.statRadosObject(baseObjectName)
			if err != nil {
				h.logger.Error("failed to list blobs", "type", blobType, "error", fmt.Errorf("stat %s: %w", baseObjectName, err))
				http.Error(rw, "internal server error", http.StatusInternalServerError)
				return
			}
			blobInfos = append(blobInfos, blobInfo{
				Name: blobID,
				Size: stat.Size,
			})
		} else {
			blobNames = append(blobNames, blobID)
		}
	}

	if err := iter.Err(); err != nil {
		h.logger.Error("failed to list blobs", "type", blobType, "error", fmt.Errorf("iterate objects: %w", err))
		http.Error(rw, "internal server error", http.StatusInternalServerError)
		return
	}

	var data []byte
	if useV2 {
		data, err = json.Marshal(blobInfos)
		if err != nil {
			h.logger.Error("failed to list blobs", "type", blobType, "error", fmt.Errorf("marshal JSON: %w", err))
			http.Error(rw, "internal server error", http.StatusInternalServerError)
			return
		}
		rw.Header().Set("Content-Type", "application/vnd.x.restic.rest.v2")
	} else {
		data, err = json.Marshal(blobNames)
		if err != nil {
			h.logger.Error("failed to list blobs", "type", blobType, "error", fmt.Errorf("marshal JSON: %w", err))
			http.Error(rw, "internal server error", http.StatusInternalServerError)
			return
		}
		rw.Header().Set("Content-Type", "application/vnd.x.restic.rest.v1")
	}

	rw.WriteHeader(http.StatusOK)
	if _, err = rw.Write(data); err != nil {
		h.logger.Warn("failed to list blobs", "type", blobType, "error", err)
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

	hctx, ok := h.openIOContext(rw, r)
	if !ok {
		return
	}
	defer hctx.Destroy()

	objectName := blobType + "/" + blobID

	if err := hctx.serveRadosObject(rw, r, objectName); err != nil {
		h.handleRadosError(rw, r, blobID, err)
	}
}

func (h *Handler) createBlob(w http.ResponseWriter, r *http.Request) {
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

	hctx, ok := h.openIOContext(rw, r)
	if !ok {
		return
	}
	defer hctx.Destroy()

	objectName := blobType + "/" + blobID

	if err := hctx.createRadosObject(rw, r, objectName, blobID, canStripeBlobType(blobType)); err != nil {
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

	hctx, ok := h.openIOContext(rw, r)
	if !ok {
		return
	}
	defer hctx.Destroy()

	objectName := blobType + "/" + blobID

	rioctx, _, err := hctx.statRadosObject(objectName)
	if errors.Is(err, rados.ErrNotFound) {
		rw.WriteHeader(http.StatusOK)
		return
	}
	if err != nil {
		h.handleRadosError(rw, r, blobID, fmt.Errorf("stat object %s: %w", objectName, err))
		return
	}

	if err := rioctx.Remove(objectName); err != nil {
		h.handleRadosError(rw, r, blobID, fmt.Errorf("delete object %s: %w", objectName, err))
		return
	}
	rw.WriteHeader(http.StatusOK)
}

type blobInfo struct {
	Name string `json:"name"`
	Size uint64 `json:"size"`
}

func acceptsBlobListV2(r *http.Request) bool {
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

func (h *Handler) setupRoutes(mux *http.ServeMux) {
	mux.HandleFunc("HEAD /config", h.getConfig)
	mux.HandleFunc("GET /config", h.getConfig)
	mux.HandleFunc("POST /config", h.createConfig)
	mux.HandleFunc("DELETE /config", h.deleteConfig)

	mux.HandleFunc("GET /{type}/", h.listBlobs)
	mux.HandleFunc("HEAD /{type}/{id}", h.getBlob)
	mux.HandleFunc("GET /{type}/{id}", h.getBlob)
	mux.HandleFunc("POST /{type}/{id}", h.createBlob)
	mux.HandleFunc("DELETE /{type}/{id}", h.deleteBlob)

	mux.HandleFunc("POST /", h.createRepo)
}

const radosReadChunkSize = 32 * 1024

func parseExpectedHash(object string) ([32]byte, error) {
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

type httpRange struct {
	start  int64
	end    int64
	status int
}

func parseRange(r *http.Request, size int64) (*httpRange, error) {
	if size == 0 {
		return &httpRange{start: 0, end: 0, status: http.StatusOK}, nil
	}

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

func (hctx *HandlerContext) serveRadosObject(w http.ResponseWriter, r *http.Request, object string) error {
	rioctx, stat, err := hctx.statRadosObject(object)
	if err != nil {
		if errors.Is(err, rados.ErrNotFound) {
			return errObjectNotFound
		}
		return fmt.Errorf("stat %s: %w", object, err)
	}

	striped := hctx.striperIO != nil && rioctx == hctx.striperIO
	hctx.logger.Debug("reading blob", "object", object, "size", stat.Size, "striped", striped)

	if stat.Size > uint64(math.MaxInt64) {
		return fmt.Errorf("object %s size exceeds max int64: %d", object, stat.Size)
	}

	rng, err := parseRange(r, int64(stat.Size))
	if err != nil {
		http.Error(w, err.Error(), http.StatusRequestedRangeNotSatisfiable)
		return err
	}

	if rng.status == http.StatusPartialContent {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", rng.start, rng.end, stat.Size))
	}

	contentLength := rng.end - rng.start + 1
	w.Header().Set("Content-Length", strconv.FormatInt(contentLength, 10))
	w.WriteHeader(rng.status)

	if r.Method == "HEAD" || contentLength == 0 {
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

		n, err := rioctx.Read(object, buffer[:chunkSize], uint64(offset))
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

func (hctx *HandlerContext) createRadosObject(w http.ResponseWriter, r *http.Request, object string, hashID string, canStripe bool) error {
	size := r.ContentLength
	useStriper := canStripe && hctx.striperIO != nil && size > hctx.maxObjectSize

	if !useStriper && size > 0 && size > hctx.maxObjectSize {
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

	actualSize := int64(len(data))

	if !useStriper && actualSize > hctx.maxObjectSize {
		return errObjectTooLarge
	}

	expected, err := parseExpectedHash(hashID)
	if err != nil {
		return err
	}

	if expected != [32]byte{} {
		actual := sha256.Sum256(data)
		if actual != expected {
			hctx.logger.Warn("input hash mismatch", "object", object, "expected", fmt.Sprintf("%x", expected), "got", fmt.Sprintf("%x", actual))
			return errHashMismatch
		}
	}

	_, _, err = hctx.statRadosObject(object)
	if err == nil {
		return errObjectExists
	}
	if !errors.Is(err, rados.ErrNotFound) {
		return fmt.Errorf("stat object %s: %w", object, err)
	}

	var rioctx RadosIOContext
	if useStriper {
		rioctx = hctx.striperIO
	} else {
		rioctx = hctx.radosIO
	}
	hctx.logger.Debug("creating blob", "object", object, "size", len(data), "striped", useStriper)

	err = rioctx.WriteFull(object, data)
	if err != nil {
		return fmt.Errorf("write object %s: %w", object, err)
	}

	w.WriteHeader(http.StatusOK)
	return nil
}

func (hctx *HandlerContext) statRadosObject(object string) (RadosIOContext, StatInfo, error) {
	if hctx.striperIO != nil {
		stat, err := hctx.radosIO.Stat(object)
		if !errors.Is(err, rados.ErrNotFound) {
			return hctx.radosIO, stat, err
		}
		_, stripeErr := hctx.radosIO.Stat(object + ".0000000000000000")
		if !errors.Is(stripeErr, rados.ErrNotFound) {
			stat, err = hctx.striperIO.Stat(object)
			return hctx.striperIO, stat, err
		}
		return hctx.radosIO, StatInfo{}, err
	}
	stat, err := hctx.radosIO.Stat(object)
	return hctx.radosIO, stat, err
}
