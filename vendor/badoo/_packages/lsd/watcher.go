package lsd

import (
	"badoo/_packages/log"
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path"

	"github.com/fsnotify/fsnotify"
)

// Watch starts to look through category's files
// and sends full file names that are ready to be processed to outChan
// if error occurs, Watch exits immediately, closing all background stuff
// watch process can be cancelled by cancelling parent ctx
// keep in mind that Watch blocks your current goroutine, so you need to use it separate one
func Watch(ctx context.Context, category Category, outChan chan string) error {

	fsWatcher, err := fsnotify.NewWatcher()
	if err != nil {
		return err
	}

	dir := category.GetDir()
	currentSymlink := category.GetCurrentSymlink()

	w := &watcher{
		ctx:            ctx,
		dir:            dir,
		currentSymlink: currentSymlink,
		fsWatcher:      fsWatcher,
		outChan:        outChan,
	}

	err = fsWatcher.Add(dir)
	if err != nil {
		w.closeFsWatcher()
		return err
	}
	return w.watch()
}

type watcher struct {
	ctx            context.Context
	dir            string
	currentSymlink string
	fsWatcher      *fsnotify.Watcher
	outChan        chan string
	lastSentName   string
}

func (w *watcher) watch() error {

	defer w.closeFsWatcher()

	err := w.pushFiles()
	if err != nil {
		return fmt.Errorf("error on old files check: %s", err)
	}

	for {
		select {
		case ev, ok := <-w.fsWatcher.Events:
			if !ok {
				return nil
			}
			if ev.Name != w.currentSymlink || ev.Op != fsnotify.Create {
				continue
			}
			err := w.pushFiles()
			if err != nil {
				return fmt.Errorf("error pushing files: %s", err)
			}
		case err, ok := <-w.fsWatcher.Errors:
			if !ok {
				return nil
			}
			return fmt.Errorf("fsnotify internal error: %s", err)
		case <-w.ctx.Done():
			return nil
		}
	}
}

func (w *watcher) closeFsWatcher() {

	// fs watcher closes it's event/error channels on close
	err := w.fsWatcher.Close()
	if err != nil {
		log.Errorf("failed to close underlying fs watcher (possible goroutine leak): %s", err)
		return
	}
	// we need to read and discard all events from underlying fs watcher
	// to make it able to exit and prevent goroutine leak
	// because it's reader can be blocked (it uses unbuffered channels for events/errors)
outerLoopEvent:
	for {
		select {
		case _, ok := <-w.fsWatcher.Events:
			if !ok {
				break outerLoopEvent
			}
		default:
			break outerLoopEvent
		}
	}
outerLoopErr:
	for {
		select {
		case _, ok := <-w.fsWatcher.Errors:
			if !ok {
				break outerLoopErr
			}
		default:
			break outerLoopErr
		}
	}
}

func (w *watcher) pushFiles() error {

	files, err := ioutil.ReadDir(w.dir)
	if err != nil {
		return fmt.Errorf("read dir failed: %s", err)
	}

	currentFile, err := os.Readlink(w.currentSymlink)
	if os.IsNotExist(err) {
		fs := make([]string, 0)
		for _, info := range files {
			fs = append(fs, info.Name())
		}
		log.Errorf("readlink race condition found: %s, files: %+v", w.currentSymlink, fs)
	}
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to read link '%s': %s", w.currentSymlink, err)
	}

	currentFileBase := path.Base(currentFile)
	currentSymlinkBase := path.Base(w.currentSymlink)

	for _, file := range files {
		name := file.Name()
		if name == currentFileBase {
			continue
		}
		if name == currentSymlinkBase {
			continue
		}
		if name > currentFileBase {
			log.Warnf("race condition found: %s if greater than %s", name, currentFileBase)
			continue
		}
		if name <= w.lastSentName {
			continue
		}
		w.outChan <- w.dir + "/" + name
		w.lastSentName = name
	}
	return nil
}
