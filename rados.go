package main

import (
	"crypto/sha256"
	"fmt"
	"hash"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ceph/go-ceph/rados"
	"github.com/ceph/go-ceph/rados/striper"
)

type StatInfo struct {
	Size    uint64
	ModTime time.Time
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
}

type radosIOContextWrapper struct {
	ioctx      *rados.IOContext
	radosCalls *uint64
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

type striperIOContextWrapper struct {
	striper    *striper.Striper
	ioctx      *rados.IOContext
	radosCalls *uint64
}

func (s *striperIOContextWrapper) Stat(object string) (StatInfo, error) {
	slog.Debug("striper.Stat", "object", object)
	atomic.AddUint64(s.radosCalls, 1)
	stat, err := s.striper.Stat(object)
	modTime := time.Unix(stat.ModTime.Sec, stat.ModTime.Nsec)
	return StatInfo{Size: stat.Size, ModTime: modTime}, err
}

func (s *striperIOContextWrapper) Read(object string, buf []byte, offset uint64) (int, error) {
	slog.Debug("striper.Read", "object", object, "offset", offset, "size", len(buf))
	atomic.AddUint64(s.radosCalls, 1)
	return s.striper.Read(object, buf, offset)
}

func (s *striperIOContextWrapper) Append(object string, data []byte) error {
	slog.Debug("striper.Append", "object", object, "size", len(data))
	atomic.AddUint64(s.radosCalls, 1)
	return s.striper.Append(object, data)
}

func (s *striperIOContextWrapper) WriteFull(object string, data []byte) error {
	slog.Debug("striper.WriteFull", "object", object, "size", len(data))
	atomic.AddUint64(s.radosCalls, 1)
	return s.striper.WriteFull(object, data)
}

func (s *striperIOContextWrapper) Remove(object string) error {
	slog.Debug("striper.Remove", "object", object)
	atomic.AddUint64(s.radosCalls, 1)
	return s.striper.Remove(object)
}

func (s *striperIOContextWrapper) Destroy() {
	slog.Debug("striper.Destroy")
	s.striper.Destroy()
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

type RadosObjectWriter struct {
	ctx           RadosIOContext
	object        string
	hasher        hash.Hash
	bufferPool    *sync.Pool
	bufferSize    int64
	alignment     uint64
	requiresAlign bool
}

func NewRadosObjectWriter(ctx RadosIOContext, object string, bufferPool *sync.Pool, bufferSize int64) (*RadosObjectWriter, error) {
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
		bufferPool:    bufferPool,
		bufferSize:    bufferSize,
		alignment:     alignment,
		requiresAlign: requiresAlign,
	}, nil
}

func (w *RadosObjectWriter) Write(p []byte) (int, error) {
	return 0, fmt.Errorf("Write called directly on RadosObjectWriter; should use ReadFrom or io.Copy")
}

func (w *RadosObjectWriter) ReadFrom(r io.Reader) (int64, error) {
	bufPtr := w.bufferPool.Get().(*[]byte)
	defer w.bufferPool.Put(bufPtr)
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

		if firstRead && isEOF && bufferOffset > 0 {
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
	size   int64
}

func NewRadosObjectReaderWithSize(ctx RadosIOContext, object string, size int64) *RadosObjectReader {
	return &RadosObjectReader{
		ctx:    ctx,
		object: object,
		size:   size,
	}
}

func (r *RadosObjectReader) ReadAt(p []byte, off int64) (int, error) {
	if off >= r.size {
		return 0, io.EOF
	}
	return r.ctx.Read(r.object, p, uint64(off))
}
