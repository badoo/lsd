package hdfs

import (
	"fmt"
	"os"
	"time"

	"path"
	"strings"

	"github.com/vladimirvivien/gowfs"
)

const CONNECT_TIMEOUT = time.Second * 10
const RESPONSE_TIMEOUT = time.Second * 10

type uploader struct {
	hostname string
	hdfs     *gowfs.FileSystem
	dstDir   string
	tmpDir   string
}

func newUploader(hostname, user, namenode, dstDir, tmpDir string) (*uploader, error) {

	hdfs, err := gowfs.NewFileSystem(gowfs.Configuration{
		Addr:                  namenode,
		User:                  user,
		ConnectionTimeout:     CONNECT_TIMEOUT,
		ResponseHeaderTimeout: RESPONSE_TIMEOUT,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to init hdfs: %v", err)
	}
	err = makeDirInHDFS(hdfs, dstDir)
	if err != nil {
		return nil, fmt.Errorf("failed to create %s in hdfs: %v", dstDir, err)
	}
	err = makeDirInHDFS(hdfs, tmpDir)
	if err != nil {
		return nil, fmt.Errorf("failed to create %s in hdfs: %v", tmpDir, err)
	}
	return &uploader{
		hdfs:     hdfs,
		hostname: hostname,
		dstDir:   strings.TrimRight(dstDir, "/"),
		tmpDir:   strings.TrimRight(tmpDir, "/"),
	}, nil
}

func (u *uploader) upload(sourcePath string) error {

	// remote file names are prefixed with <hostname>_%s
	remoteFileName := u.hostname + "_" + path.Base(sourcePath)

	tmpFilePath := gowfs.Path{Name: u.tmpDir + "/" + remoteFileName}
	dstFilePath := gowfs.Path{Name: u.dstDir + "/" + remoteFileName}

	fp, err := os.Open(sourcePath)
	if err != nil {
		return fmt.Errorf("failed to open %s: %v", sourcePath, err)
	}
	stat, err := fp.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat %s: %v", sourcePath, err)
	}

	_, err = u.hdfs.Create(fp, tmpFilePath, true, 0, 0, 0775, 0)
	if err != nil {
		return fmt.Errorf("failed to create tmp file %v in hdfs: %v", tmpFilePath, err)
	}
	// hadoop accepts time in milliseconds
	_, err = u.hdfs.SetTimes(tmpFilePath, -1, stat.ModTime().Unix()*1000)
	if err != nil {
		return fmt.Errorf("failed to set mtime in hdfs: %v", err)
	}
	_, err = u.hdfs.Rename(tmpFilePath, dstFilePath)
	if err != nil {
		return fmt.Errorf("failed to rename %v => %v in hdfs: %v", tmpFilePath, dstFilePath, err)
	}
	return nil
}

func makeDirInHDFS(h *gowfs.FileSystem, dirname string) (err error) {
	_, err = h.MkDirs(
		gowfs.Path{Name: dirname},
		0775,
	)
	return
}
