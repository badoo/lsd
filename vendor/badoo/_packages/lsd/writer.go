package lsd

import (
	"badoo/_packages/log"
	"fmt"
	"os"
	"path"
	"syscall"
)

type Writer interface {
	WriteString(eventLine string) error
	WriteBytes(eventBytes []byte) error
	WriteJson(event interface{}) error
}

func doWrite(filename string, flags int, eventBytes []byte, exclusive bool) error {

	fp, err := os.OpenFile(filename, flags, 0666)
	if err != nil && os.IsNotExist(err) {
		err = ensureDirExists(path.Dir(filename))
		if err != nil {
			return fmt.Errorf("failed to ensure that directory exists: %v", err)
		}
		fp, err = os.OpenFile(filename, flags, 0666)
	}
	if err != nil {
		return fmt.Errorf("failed to open %s: %v", filename, err)
	}
	defer func() {
		closeErr := fp.Close()
		if closeErr != nil {
			log.Errorf("failed to close %s after write: %v", filename, closeErr)
		}
	}()

	if exclusive {
		err := syscall.Flock(int(fp.Fd()), syscall.LOCK_SH)
		if err != nil {
			return fmt.Errorf("failed to lock %s for write: %v", filename, err)
		}
	}
	_, err = fp.Write(eventBytes)
	if exclusive {
		unlockErr := syscall.Flock(int(fp.Fd()), syscall.LOCK_UN)
		if unlockErr != nil {
			log.Errorf("failed to unlock %s: %v\n", filename, err)
		}
	}
	return err
}

func ensureDirExists(dirPath string) error {

	info, err := os.Stat(dirPath)
	if err == nil {
		// os.Stat does resolve symlinks
		if info.IsDir() {
			return nil
		}
		return fmt.Errorf("%s is not a directory", dirPath)
	}
	if !os.IsNotExist(err) {
		return fmt.Errorf("failed to stat dir %s: %v", dirPath, err)
	}
	err = os.MkdirAll(dirPath, 0777)
	if os.IsExist(err) {
		return nil
	}
	return fmt.Errorf("mkdir failed for %s: %v", dirPath, err)
}
