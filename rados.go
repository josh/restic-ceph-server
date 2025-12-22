package main

import (
	"crypto/sha256"
	"hash"
	"io"
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
	Write(object string, data []byte, offset uint64) error
	Remove(object string) error
	Destroy()
	Iter() (*rados.Iter, error)
}

type radosIOContextWrapper struct {
	ioctx      *rados.IOContext
	radosCalls *uint64
}

func (r *radosIOContextWrapper) Stat(object string) (StatInfo, error) {
	atomic.AddUint64(r.radosCalls, 1)
	stat, err := r.ioctx.Stat(object)
	return StatInfo{Size: stat.Size, ModTime: stat.ModTime}, err
}

func (r *radosIOContextWrapper) Read(object string, buf []byte, offset uint64) (int, error) {
	atomic.AddUint64(r.radosCalls, 1)
	return r.ioctx.Read(object, buf, offset)
}

func (r *radosIOContextWrapper) Write(object string, data []byte, offset uint64) error {
	atomic.AddUint64(r.radosCalls, 1)
	return r.ioctx.Write(object, data, offset)
}

func (r *radosIOContextWrapper) Remove(object string) error {
	atomic.AddUint64(r.radosCalls, 1)
	return r.ioctx.Delete(object)
}

func (r *radosIOContextWrapper) Destroy() {
	r.ioctx.Destroy()
}

func (r *radosIOContextWrapper) Iter() (*rados.Iter, error) {
	atomic.AddUint64(r.radosCalls, 1)
	return r.ioctx.Iter()
}

type striperIOContextWrapper struct {
	striper    *striper.Striper
	radosCalls *uint64
}

func (s *striperIOContextWrapper) Stat(object string) (StatInfo, error) {
	atomic.AddUint64(s.radosCalls, 1)
	stat, err := s.striper.Stat(object)
	modTime := time.Unix(stat.ModTime.Sec, stat.ModTime.Nsec)
	return StatInfo{Size: stat.Size, ModTime: modTime}, err
}

func (s *striperIOContextWrapper) Read(object string, buf []byte, offset uint64) (int, error) {
	atomic.AddUint64(s.radosCalls, 1)
	return s.striper.Read(object, buf, offset)
}

func (s *striperIOContextWrapper) Write(object string, data []byte, offset uint64) error {
	atomic.AddUint64(s.radosCalls, 1)
	return s.striper.Write(object, data, offset)
}

func (s *striperIOContextWrapper) Remove(object string) error {
	atomic.AddUint64(s.radosCalls, 1)
	return s.striper.Remove(object)
}

func (s *striperIOContextWrapper) Destroy() {
	s.striper.Destroy()
}

func (s *striperIOContextWrapper) Iter() (*rados.Iter, error) {
	panic("Iter() not supported on striper")
}

type RadosObjectWriter struct {
	ctx    RadosIOContext
	object string
	offset int64
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
	if err := w.ctx.Write(w.object, p, uint64(w.offset)); err != nil {
		return 0, err
	}
	w.offset += int64(len(p))
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
