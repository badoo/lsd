package files

import (
	"fmt"
	"os"
	"syscall"
)

type FileInfo struct {
	os.FileInfo
	inode uint64
}

// just to bee similar with os.FileInfo interface
func (st FileInfo) Inode() uint64 {
	return st.inode
}

func (st FileInfo) IsLink() bool {
	return st.Mode()&os.ModeSymlink != 0
}

func StatFp(fp *os.File) (FileInfo, error) {
	st, err := fp.Stat()
	if err != nil {
		return FileInfo{}, err
	}
	return wrapStat(st)
}

func Lstat(name string) (FileInfo, error) {
	st, err := os.Lstat(name)
	if err != nil {
		return FileInfo{}, err
	}
	return wrapStat(st)
}

func Stat(name string) (FileInfo, error) {
	st, err := os.Stat(name)
	if err != nil {
		return FileInfo{}, err
	}
	return wrapStat(st)
}

func wrapStat(st os.FileInfo) (FileInfo, error) {

	posixStat, ok := st.Sys().(*syscall.Stat_t)
	if !ok {
		return FileInfo{}, fmt.Errorf("failed to get posix stat for %s (possibly unsupported OS)", st.Name())
	}
	info := FileInfo{
		FileInfo: st,
		inode:    posixStat.Ino,
	}
	if !st.IsDir() && !st.Mode().IsRegular() && !info.IsLink() {
		return info, fmt.Errorf("%s is not a dir/symlink/regular file", st.Name())
	}
	return info, nil
}
