package client

import (
	"badoo/_packages/log"
	"fmt"
	"io"
	"os"

	"time"

	"github.com/fsnotify/fsnotify"
)

const READDIR_BUFFER_SIZE = 1000

func ReadDirIntoChannel(baseDir string, events chan fsnotify.Event, iterateSubDir bool) error {

	fp, err := os.Open(baseDir)
	if err != nil {
		return fmt.Errorf("failed to open dir %s: %v", baseDir, err)
	}
	defer func() {
		err = fp.Close()
		if err != nil {
			log.Errorf("failed to close %s: %v", fp.Name(), err)
		}
	}()
	for {
		fis, err := fp.Readdir(READDIR_BUFFER_SIZE)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return fmt.Errorf("failed to read dir %s: %v", baseDir, err)
		}
		for _, fi := range fis {
			if fi.Name() == "." || fi.Name() == ".." {
				continue
			}
			events <- fsnotify.Event{
				Name: baseDir + string(os.PathSeparator) + fi.Name(),
				Op:   fsnotify.Create,
			}
			if !fi.IsDir() || !iterateSubDir {
				continue
			}
			err = ReadDirIntoChannel(baseDir+string(os.PathSeparator)+fi.Name(), events, false) // only two levels are supported
			if err != nil {
				return err
			}
		}
	}
}

func int2sec(d uint64) time.Duration {
	return time.Duration(d) * time.Second
}
