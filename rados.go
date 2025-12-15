package main

import (
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
	WriteFull(object string, data []byte) error
	Remove(object string) error
	Destroy()
	Iter() (*rados.Iter, error)
}

type radosIOContextWrapper struct {
	ioctx *rados.IOContext
}

func (r *radosIOContextWrapper) Stat(object string) (StatInfo, error) {
	stat, err := r.ioctx.Stat(object)
	return StatInfo{Size: stat.Size, ModTime: stat.ModTime}, err
}

func (r *radosIOContextWrapper) Read(object string, buf []byte, offset uint64) (int, error) {
	return r.ioctx.Read(object, buf, offset)
}

func (r *radosIOContextWrapper) WriteFull(object string, data []byte) error {
	writeOp := rados.CreateWriteOp()
	defer writeOp.Release()
	writeOp.Create(rados.CreateExclusive)
	writeOp.SetAllocationHint(uint64(len(data)), uint64(len(data)), rados.AllocHintIncompressible|rados.AllocHintImmutable|rados.AllocHintLonglived)
	writeOp.WriteFull(data)
	return writeOp.Operate(r.ioctx, object, rados.OperationNoFlag)
}

func (r *radosIOContextWrapper) Remove(object string) error {
	return r.ioctx.Delete(object)
}

func (r *radosIOContextWrapper) Destroy() {
	r.ioctx.Destroy()
}

func (r *radosIOContextWrapper) Iter() (*rados.Iter, error) {
	return r.ioctx.Iter()
}

type striperIOContextWrapper struct {
	striper *striper.Striper
}

func (s *striperIOContextWrapper) Stat(object string) (StatInfo, error) {
	stat, err := s.striper.Stat(object)
	modTime := time.Unix(stat.ModTime.Sec, stat.ModTime.Nsec)
	return StatInfo{Size: stat.Size, ModTime: modTime}, err
}

func (s *striperIOContextWrapper) Read(object string, buf []byte, offset uint64) (int, error) {
	return s.striper.Read(object, buf, offset)
}

func (s *striperIOContextWrapper) WriteFull(object string, data []byte) error {
	return s.striper.WriteFull(object, data)
}

func (s *striperIOContextWrapper) Remove(object string) error {
	return s.striper.Remove(object)
}

func (s *striperIOContextWrapper) Destroy() {
	s.striper.Destroy()
}

func (s *striperIOContextWrapper) Iter() (*rados.Iter, error) {
	panic("Iter() not supported on striper")
}
