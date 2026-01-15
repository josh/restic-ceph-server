package main

import (
	"crypto/sha256"
	"fmt"
	"hash"
	"io"
	"log/slog"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ceph/go-ceph/rados"
)

type StatInfo struct {
	Size    uint64
	ModTime time.Time
}

type BufferPool struct {
	pool *sync.Pool
	size int64
}

func NewBufferPool(size int64) *BufferPool {
	return &BufferPool{
		pool: &sync.Pool{
			New: func() interface{} {
				buf := make([]byte, size)
				return &buf
			},
		},
		size: size,
	}
}

func (bp *BufferPool) Get() *[]byte {
	return bp.pool.Get().(*[]byte)
}

func (bp *BufferPool) Put(bufPtr *[]byte) {
	bp.pool.Put(bufPtr)
}

func (bp *BufferPool) Size() int64 {
	return bp.size
}

type RadosIOContext interface {
	Stat(object string) (StatInfo, error)
	Read(object string, buf []byte, offset uint64) (int, error)
	Append(object string, data []byte) error
	WriteFull(object string, data []byte) error
	Remove(object string) error
	Destroy()
	Iter() (*rados.Iter, error)
	Alignment() (uint64, error)
	RequiresAlignment() (bool, error)
	ReadBufferPool() *BufferPool
	WriteBufferPool() *BufferPool
}

type radosIOContextWrapper struct {
	ioctx       *rados.IOContext
	radosCalls  *uint64
	readBuffer  *BufferPool
	writeBuffer *BufferPool
}

func (r *radosIOContextWrapper) Stat(object string) (StatInfo, error) {
	slog.Debug("rados.Stat", "object", object)
	atomic.AddUint64(r.radosCalls, 1)
	stat, err := r.ioctx.Stat(object)
	return StatInfo{Size: stat.Size, ModTime: stat.ModTime}, err
}

func (r *radosIOContextWrapper) Read(object string, buf []byte, offset uint64) (int, error) {
	slog.Debug("rados.Read", "object", object, "offset", offset, "size", len(buf))
	atomic.AddUint64(r.radosCalls, 1)
	return r.ioctx.Read(object, buf, offset)
}

func (r *radosIOContextWrapper) Append(object string, data []byte) error {
	slog.Debug("rados.Append", "object", object, "size", len(data))
	atomic.AddUint64(r.radosCalls, 1)
	return r.ioctx.Append(object, data)
}

func (r *radosIOContextWrapper) WriteFull(object string, data []byte) error {
	slog.Debug("rados.WriteFull", "object", object, "size", len(data))
	atomic.AddUint64(r.radosCalls, 1)
	return r.ioctx.WriteFull(object, data)
}

func (r *radosIOContextWrapper) Remove(object string) error {
	slog.Debug("rados.Remove", "object", object)
	atomic.AddUint64(r.radosCalls, 1)
	return r.ioctx.Delete(object)
}

func (r *radosIOContextWrapper) Destroy() {
	slog.Debug("rados.Destroy")
	r.ioctx.Destroy()
}

func (r *radosIOContextWrapper) Iter() (*rados.Iter, error) {
	slog.Debug("rados.Iter")
	atomic.AddUint64(r.radosCalls, 1)
	return r.ioctx.Iter()
}

func (r *radosIOContextWrapper) Alignment() (uint64, error) {
	return r.ioctx.Alignment()
}

func (r *radosIOContextWrapper) RequiresAlignment() (bool, error) {
	return r.ioctx.RequiresAlignment()
}

func (r *radosIOContextWrapper) ReadBufferPool() *BufferPool {
	return r.readBuffer
}

func (r *radosIOContextWrapper) WriteBufferPool() *BufferPool {
	return r.writeBuffer
}

const (
	xattrStripeUnit  = "striper.layout.stripe_unit"
	xattrStripeCount = "striper.layout.stripe_count"
	xattrObjectSize  = "striper.layout.object_size"
	xattrSize        = "striper.size"
)

type striperIOContextWrapper struct {
	ioctx       *rados.IOContext
	objectSize  uint64
	radosCalls  *uint64
	readBuffer  *BufferPool
	writeBuffer *BufferPool
}

func (s *striperIOContextWrapper) getObjectID(soid string, objectno uint64) string {
	return fmt.Sprintf("%s.%016x", soid, objectno)
}

func (s *striperIOContextWrapper) Stat(object string) (StatInfo, error) {
	slog.Debug("striper.Stat", "object", object)

	firstObjID := s.getObjectID(object, 0)
	atomic.AddUint64(s.radosCalls, 1)
	stat, err := s.ioctx.Stat(firstObjID)
	if err != nil {
		return StatInfo{}, err
	}

	sizeAttr := make([]byte, 32)
	atomic.AddUint64(s.radosCalls, 1)
	n, err := s.ioctx.GetXattr(firstObjID, xattrSize, sizeAttr)
	if err != nil {
		return StatInfo{}, fmt.Errorf("get size xattr: %w", err)
	}

	size, err := strconv.ParseUint(string(sizeAttr[:n]), 10, 64)
	if err != nil {
		return StatInfo{}, fmt.Errorf("parse size xattr: %w", err)
	}

	return StatInfo{Size: size, ModTime: stat.ModTime}, nil
}

func (s *striperIOContextWrapper) Read(object string, buf []byte, offset uint64) (int, error) {
	slog.Debug("striper.Read", "object", object, "offset", offset, "size", len(buf))

	stat, err := s.Stat(object)
	if err != nil {
		return 0, err
	}

	totalSize := stat.Size
	if offset >= totalSize {
		return 0, io.EOF
	}

	readLen := uint64(len(buf))
	if offset+readLen > totalSize {
		readLen = totalSize - offset
	}

	totalRead := 0
	remaining := int(readLen)
	currentOffset := offset

	for remaining > 0 {
		objectNo := currentOffset / s.objectSize
		objectOffset := currentOffset % s.objectSize

		availableInObject := s.objectSize - objectOffset
		toRead := remaining
		if uint64(toRead) > availableInObject {
			toRead = int(availableInObject)
		}

		objectID := s.getObjectID(object, objectNo)
		atomic.AddUint64(s.radosCalls, 1)
		n, err := s.ioctx.Read(objectID, buf[totalRead:totalRead+toRead], objectOffset)
		if err != nil && err != io.EOF {
			return totalRead, err
		}

		totalRead += n
		remaining -= n
		currentOffset += uint64(n)

		if n == 0 || err == io.EOF {
			break
		}
	}

	return totalRead, nil
}

func (s *striperIOContextWrapper) Append(object string, data []byte) error {
	slog.Debug("striper.Append", "object", object, "size", len(data))

	appendSize := uint64(len(data))
	if appendSize == 0 {
		return nil
	}

	firstObjID := s.getObjectID(object, 0)
	objectSizeStr := strconv.FormatUint(s.objectSize, 10)

	var currentSize uint64
	sizeAttr := make([]byte, 32)
	atomic.AddUint64(s.radosCalls, 1)
	n, err := s.ioctx.GetXattr(firstObjID, xattrSize, sizeAttr)
	if err == nil {
		currentSize, _ = strconv.ParseUint(string(sizeAttr[:n]), 10, 64)
	} else if err.Error() == rados.ErrNotFound.Error() {
		op := rados.CreateWriteOp()
		op.Create(rados.CreateExclusive)
		op.SetXattr(xattrStripeUnit, []byte(objectSizeStr))
		op.SetXattr(xattrStripeCount, []byte("1"))
		op.SetXattr(xattrObjectSize, []byte(objectSizeStr))
		op.SetXattr(xattrSize, []byte("0"))

		atomic.AddUint64(s.radosCalls, 1)
		opErr := op.Operate(s.ioctx, firstObjID, rados.OperationNoFlag)
		op.Release()
		if opErr != nil && opErr.Error() != rados.ErrObjectExists.Error() {
			return fmt.Errorf("create first object: %w", opErr)
		}
		currentSize = 0
	} else {
		return fmt.Errorf("get current size: %w", err)
	}

	newSize := currentSize + appendSize
	writeOffset := currentSize
	written := uint64(0)

	for written < appendSize {
		objectNo := writeOffset / s.objectSize
		objectOffset := writeOffset % s.objectSize

		availableInObject := s.objectSize - objectOffset
		toWrite := appendSize - written
		if toWrite > availableInObject {
			toWrite = availableInObject
		}

		objectID := s.getObjectID(object, objectNo)
		atomic.AddUint64(s.radosCalls, 1)
		err := s.ioctx.Write(objectID, data[written:written+toWrite], objectOffset)
		if err != nil {
			return fmt.Errorf("write to object %d: %w", objectNo, err)
		}

		written += toWrite
		writeOffset += toWrite
	}

	atomic.AddUint64(s.radosCalls, 1)
	err = s.ioctx.SetXattr(firstObjID, xattrSize, []byte(strconv.FormatUint(newSize, 10)))
	if err != nil {
		return fmt.Errorf("update size xattr: %w", err)
	}

	return nil
}

func (s *striperIOContextWrapper) WriteFull(object string, data []byte) error {
	slog.Debug("striper.WriteFull", "object", object, "size", len(data))
	_ = s.Remove(object)
	return s.Append(object, data)
}

func (s *striperIOContextWrapper) Remove(object string) error {
	slog.Debug("striper.Remove", "object", object)

	firstObjID := s.getObjectID(object, 0)

	sizeAttr := make([]byte, 32)
	atomic.AddUint64(s.radosCalls, 1)
	n, err := s.ioctx.GetXattr(firstObjID, xattrSize, sizeAttr)
	if err != nil {
		if err.Error() == rados.ErrNotFound.Error() {
			return rados.ErrNotFound
		}
		return fmt.Errorf("get size xattr: %w", err)
	}

	totalSize, err := strconv.ParseUint(string(sizeAttr[:n]), 10, 64)
	if err != nil {
		return fmt.Errorf("parse size: %w", err)
	}

	numObjects := uint64(1)
	if totalSize > 0 {
		numObjects = (totalSize + s.objectSize - 1) / s.objectSize
	}

	for i := int64(numObjects - 1); i >= 1; i-- {
		objectID := s.getObjectID(object, uint64(i))
		atomic.AddUint64(s.radosCalls, 1)
		err := s.ioctx.Delete(objectID)
		if err != nil && err.Error() != rados.ErrNotFound.Error() {
			return fmt.Errorf("delete object %d: %w", i, err)
		}
	}

	atomic.AddUint64(s.radosCalls, 1)
	err = s.ioctx.Delete(firstObjID)
	if err != nil {
		return fmt.Errorf("delete first object: %w", err)
	}

	return nil
}

func (s *striperIOContextWrapper) Destroy() {
}

func (s *striperIOContextWrapper) Iter() (*rados.Iter, error) {
	panic("Iter() not supported on striper")
}

func (s *striperIOContextWrapper) Alignment() (uint64, error) {
	return s.ioctx.Alignment()
}

func (s *striperIOContextWrapper) RequiresAlignment() (bool, error) {
	return s.ioctx.RequiresAlignment()
}

func (s *striperIOContextWrapper) ReadBufferPool() *BufferPool {
	return s.readBuffer
}

func (s *striperIOContextWrapper) WriteBufferPool() *BufferPool {
	return s.writeBuffer
}

type RadosObjectWriter struct {
	ctx           RadosIOContext
	object        string
	hasher        hash.Hash
	alignment     uint64
	requiresAlign bool
}

func NewRadosObjectWriter(ctx RadosIOContext, object string) (*RadosObjectWriter, error) {
	requiresAlign, err := ctx.RequiresAlignment()
	if err != nil {
		requiresAlign = false
	}

	alignment := uint64(1)
	if requiresAlign {
		alignment, err = ctx.Alignment()
		if err != nil {
			alignment = 1
			requiresAlign = false
		}
	}

	if requiresAlign && alignment > 1 {
		bufferSize := ctx.WriteBufferPool().Size()
		if bufferSize%int64(alignment) != 0 {
			slog.Warn("write buffer size not aligned to required alignment",
				"buffer_size", bufferSize,
				"alignment", alignment,
				"action", "server admin should reconfigure WriteBufferSize")
		}
	}

	return &RadosObjectWriter{
		ctx:           ctx,
		object:        object,
		hasher:        sha256.New(),
		alignment:     alignment,
		requiresAlign: requiresAlign,
	}, nil
}

func (w *RadosObjectWriter) ReadFrom(r io.Reader) (int64, error) {
	bufPtr := w.ctx.WriteBufferPool().Get()
	defer w.ctx.WriteBufferPool().Put(bufPtr)
	buffer := *bufPtr

	totalRead := int64(0)
	bufferOffset := 0
	firstRead := true

	for {
		n, readErr := r.Read(buffer[bufferOffset:])
		bufferOffset += n
		totalRead += int64(n)

		isEOF := (readErr == io.EOF)
		if readErr != nil && !isEOF {
			return totalRead, readErr
		}

		if firstRead && isEOF && bufferOffset >= 0 {
			data := buffer[:bufferOffset]
			w.hasher.Write(data)
			if err := w.ctx.WriteFull(w.object, data); err != nil {
				return totalRead, fmt.Errorf("write full object %s: %w", w.object, err)
			}
			return totalRead, nil
		}
		firstRead = false

		shouldFlush := (bufferOffset == len(buffer)) || isEOF
		if !shouldFlush {
			continue
		}

		bytesToWrite := bufferOffset
		if w.requiresAlign && w.alignment > 1 && !isEOF {
			bytesToWrite = (bufferOffset / int(w.alignment)) * int(w.alignment)
			if bytesToWrite == 0 {
				if bufferOffset == len(buffer) {
					return totalRead, fmt.Errorf("buffer size %d is smaller than required alignment %d", len(buffer), w.alignment)
				}
				continue
			}
		}

		data := buffer[:bytesToWrite]
		w.hasher.Write(data)
		if err := w.ctx.Append(w.object, data); err != nil {
			return totalRead, fmt.Errorf("append to object %s: %w", w.object, err)
		}

		remainder := bufferOffset - bytesToWrite
		if remainder > 0 {
			copy(buffer[0:remainder], buffer[bytesToWrite:bufferOffset])
		}
		bufferOffset = remainder

		if isEOF {
			return totalRead, nil
		}
	}
}

func (w *RadosObjectWriter) Sum() [32]byte {
	return [32]byte(w.hasher.Sum(nil))
}

type RadosObjectReader struct {
	ctx    RadosIOContext
	object string
	offset int64
	limit  int64
}

func NewRadosObjectReader(ctx RadosIOContext, object string, offset, length int64) *RadosObjectReader {
	return &RadosObjectReader{
		ctx:    ctx,
		object: object,
		offset: offset,
		limit:  length,
	}
}

func (r *RadosObjectReader) WriteTo(w io.Writer) (int64, error) {
	bufPtr := r.ctx.ReadBufferPool().Get()
	defer r.ctx.ReadBufferPool().Put(bufPtr)
	buffer := *bufPtr

	totalWritten := int64(0)
	currentOffset := r.offset
	remaining := r.limit

	for remaining > 0 {
		toRead := int64(len(buffer))
		if toRead > remaining {
			toRead = remaining
		}

		n, err := r.ctx.Read(r.object, buffer[:toRead], uint64(currentOffset))
		if err != nil && err != io.EOF {
			return totalWritten, fmt.Errorf("read %s at offset %d: %w", r.object, currentOffset, err)
		}

		if n > 0 {
			written, err := w.Write(buffer[:n])
			totalWritten += int64(written)
			if err != nil {
				return totalWritten, err
			}
			if written != n {
				return totalWritten, io.ErrShortWrite
			}
			currentOffset += int64(n)
			remaining -= int64(n)
		}

		if err == io.EOF || n == 0 {
			break
		}
	}

	return totalWritten, nil
}
