package util

import (
	"badoo/_packages/log"
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"strings"
	"syscall"
	"time"
)

// Writer is used for events and data structures streaming
// best way to send events to LSD
// it writes events into <BaseDir>/<category>/<yyyymmddhhmm>.log files
// it also guarantees consistency for long lines (>4k)
// provides escaping of line breaks inside events
// (single event will always be sent as single line)
// for compatibility with badoo php library, we have to use
// analog of PHP's addcslashes($message, "\\\n") for escaping of lines
// so it's not safe to send raw binary data (without any encoding)
type Writer struct {
	Category Category
}

func (w *Writer) WriteJson(data interface{}) error {

	eventBytes, err := json.Marshal(data)
	if err != nil {
		return err
	}
	_, err = w.Write(eventBytes)
	return err
}

func (w *Writer) Write(eventBytes []byte) (int, error) {

	eventBytes = append(escapeLine(bytes.TrimRight(eventBytes, "\n")), '\n')

	now := time.Now()
	filenamePrefix := fmt.Sprintf(
		"%s/%s/%04d%02d%02d%02d%02d",
		strings.TrimRight(w.Category.BaseDir, "/"),
		w.Category.Name,
		now.Year(),
		now.Month(),
		now.Day(),
		now.Hour(),
		now.Minute(),
	)

	flags := os.O_CREATE | os.O_WRONLY | os.O_APPEND
	exclusive := false
	if len(eventBytes) > 4096 {
		// after 4k os doesn't guarantee atomic writes
		filenamePrefix += "_big"
		exclusive = true
	}

	filename := filenamePrefix + ".log"

	fp, err := os.OpenFile(filename, flags, 0666)
	if err != nil && os.IsNotExist(err) {
		err = ensureDirExists(path.Dir(filename))
		if err != nil {
			return 0, fmt.Errorf("failed to ensure that directory exists: %v", err)
		}
		fp, err = os.OpenFile(filename, flags, 0666)
	}
	if err != nil {
		return 0, fmt.Errorf("failed to open %s: %v", filename, err)
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
			return 0, fmt.Errorf("failed to lock %s for write: %v", filename, err)
		}
	}

	n, err := fp.Write(eventBytes)
	if exclusive {
		unlockErr := syscall.Flock(int(fp.Fd()), syscall.LOCK_UN)
		if unlockErr != nil {
			return n, fmt.Errorf("failed to unlock %s: %v\n", filename, err)
		}
	}
	return n, err
}

// does same as PHP's addcslashes($message, "\\\n")
// for compatibility with badoo PHP library
func escapeLine(line []byte) []byte {
	return []byte(strings.NewReplacer("\n", "\\n", "\\", "\\\\").Replace(string(line)))
}

func UnescapeLine(line []byte) []byte {
	return []byte(strings.NewReplacer("\\n", "\n", "\\\\", "\\").Replace(string(line)))
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
