package main

import (
	"crypto/sha256"
	"hash"
	"io"
	"log/slog"
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

type striperIOContextWrapper struct {
	striper    *striper.Striper
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

type RadosObjectWriter struct {
	ctx    RadosIOContext
	object string
	hasher hash.Hash
}

func NewRadosObjectWriter(ctx RadosIOContext, object string) *RadosObjectWriter {
	return &RadosObjectWriter{
		ctx:    ctx,
		object: object,
		hasher: sha256.New(),
	}
}

func (w *RadosObjectWriter) Write(p []byte) (int, error) {
	w.hasher.Write(p)
	if err := w.ctx.Append(w.object, p); err != nil {
		return 0, err
	}
	return len(p), nil
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
