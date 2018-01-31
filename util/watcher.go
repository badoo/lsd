package util

import (
	"badoo/_packages/log"
	"context"
	"fmt"
	"os"
	"path"

	"io"
	"sort"
	"time"

	"strings"

	"github.com/fsnotify/fsnotify"
)

const READDIR_BUFFER_SIZE = 10000
const IN_OPERATION_CLEAN_INTERVAL = time.Second * 10

// Watch starts to look through category's files
// and sends full file names that are ready to be processed to outChan
// if error occurs, Watch exits immediately, closing all background stuff
// watch process can be cancelled by cancelling parent ctx
// keep in mind that Watch blocks your current goroutine, so you need to use it separate one
func Watch(ctx context.Context, category Category, outChan chan string) error {

	fsWatcher, err := fsnotify.NewWatcher()
	if err != nil {
		return fmt.Errorf("failed to create fsnotify watcher: %v", err)
	}

	dir := category.GetDir()
	currentSymlink := category.GetCurrentSymlink()

	w := &watcher{
		ctx:                ctx,
		dir:                dir,
		currentFileSymlink: currentSymlink,
		fsWatcher:          fsWatcher,
		outChan:            outChan,
		inOperation:        make(map[string]bool),
		categoryName:       category.Name,
	}

	err = fsWatcher.Add(dir)
	if err != nil {
		w.closeFsWatcher()
		return fmt.Errorf("failed to add %s to watch: %v", dir, err)
	}
	return w.watch()
}

type watcher struct {
	ctx                context.Context
	categoryName       string
	dir                string // no trailing /
	currentFileSymlink string
	fsWatcher          *fsnotify.Watcher
	outChan            chan string
	inOperation        map[string]bool
}

func (w *watcher) watch() error {

	defer w.closeFsWatcher()

	err := w.pushFiles()
	if err != nil {
		return fmt.Errorf("error on old files push: %v", err)
	}

	cleanTicker := time.NewTicker(IN_OPERATION_CLEAN_INTERVAL)
	defer cleanTicker.Stop()

	for {
		select {
		case ev, ok := <-w.fsWatcher.Events:
			if !ok {
				return nil
			}
			if ev.Name != w.currentFileSymlink || ev.Op != fsnotify.Create {
				continue
			}
			err := w.pushFiles()
			if err != nil {
				return fmt.Errorf("error pushing files: %v", err)
			}
		case <-cleanTicker.C:
			err := w.cleanInOperationMap()
			if err != nil {
				return fmt.Errorf("failed to clean 'in operation' map: %v", err)
			}
		case err, ok := <-w.fsWatcher.Errors:
			if !ok {
				return nil
			}
			return fmt.Errorf("fsnotify internal error: %v", err)
		case <-w.ctx.Done():
			return nil
		}
	}
}

func (w *watcher) closeFsWatcher() {

	// we need to read and discard all events from underlying fs watcher
	// to make it able to exit and prevent goroutine leak
	// because it's reader can be blocked (it uses unbuffered channels for events/errors)
	go func() {
		for range w.fsWatcher.Events {
		}
	}()
	go func() {
		for range w.fsWatcher.Errors {
		}
	}()
	err := w.fsWatcher.Close()
	if err != nil {
		log.Errorf("failed to close underlying fs watcher (possible goroutine leak): %s", err)
	}
}

func (w *watcher) pushFiles() error {

	fd, err := os.Open(w.dir)
	if err != nil {
		return fmt.Errorf("failed to open %s: %v", w.dir, err)
	}

	currentFile, err := os.Readlink(w.currentFileSymlink)
	if err != nil {
		return fmt.Errorf("failed to read current symlink %s: %v", w.currentFileSymlink, err)
	}

	currentFileName := path.Base(currentFile)
	currentSymlinkName := path.Base(w.currentFileSymlink)

	for {
		names, err := fd.Readdirnames(READDIR_BUFFER_SIZE)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return fmt.Errorf("failed to read dir names: %v", err)
		}
		sort.Strings(names)
		for _, name := range names {
			if name == currentSymlinkName {
				continue
			}
			if name >= currentFileName {
				// >= because we can have
				// file1
				// current => file2
				// file3
				// when new files is just created
				continue
			}
			if w.inOperation[name] {
				continue
			}
			if !strings.HasPrefix(name, w.categoryName) {
				// some garbage file
				continue
			}
			w.inOperation[name] = true
			w.outChan <- w.dir + "/" + name
		}
	}
}

func (w *watcher) cleanInOperationMap() error {
	for name := range w.inOperation {
		filename := w.dir + "/" + name
		_, err := os.Lstat(filename)
		if os.IsNotExist(err) {
			delete(w.inOperation, name)
			continue
		}
		if err != nil {
			return fmt.Errorf("failed to stat %s: %v", filename, err)
		}
	}
	return nil
}
