package client

import (
	"badoo/_packages/log"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"time"

	"github.com/badoo/lsd/internal/client/offsets"
	lsdProto "github.com/badoo/lsd/proto"

	"github.com/badoo/lsd/internal/client/category"

	"github.com/badoo/lsd/internal/client/usage"

	"github.com/badoo/lsd/internal/client/files"

	"context"
	"sync"

	"github.com/badoo/lsd/internal/traffic"

	"github.com/fsnotify/fsnotify"
)

const FINAL_SYNC_BUFFER_SIZE = 1000

func newFsRouter(config *lsdProto.LsdConfigClientConfigT, tm *traffic.Manager, netRouter *NetworkRouter, offsetsDb *offsets.Db) (*fsRouter, error) {

	if !filepath.IsAbs(config.GetSourceDir()) {
		return nil, fmt.Errorf("non-absolute source dir passed %s", config.GetSourceDir())
	}
	baseDir := strings.TrimSuffix(config.GetSourceDir(), string(os.PathSeparator))

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, fmt.Errorf("failed to create fsnotify watcher: %v", err)
	}

	err = watcher.Add(baseDir)
	if err != nil {
		return nil, fmt.Errorf("failed to start watching base dir %s: %v", baseDir, err)
	}

	return &fsRouter{
		config:          config,
		offsetsDb:       offsetsDb,
		baseDir:         baseDir,
		watcher:         watcher,
		alreadyWatching: make(map[string]bool),
		listeners:       make(map[string]*category.Listener),
		netRouter:       netRouter,
		usageChecker:    usage.NewChecker(),
		backlog:         newBacklog(),
		finalSyncCh:     make(chan category.FinalSyncRequest, FINAL_SYNC_BUFFER_SIZE),
		trafficManager:  tm,
	}, nil
}

type fsRouter struct {
	config          *lsdProto.LsdConfigClientConfigT
	offsetsDb       *offsets.Db
	baseDir         string
	watcher         *fsnotify.Watcher
	alreadyWatching map[string]bool // just to keep in mind which dirs we are watching

	listeners       map[string]*category.Listener
	listenersCtx    context.Context
	listenersWg     sync.WaitGroup
	cancelListeners context.CancelFunc

	netRouter      *NetworkRouter
	usageChecker   *usage.Checker
	backlog        *backlog
	finalSyncCh    chan category.FinalSyncRequest
	trafficManager *traffic.Manager
}

func (r *fsRouter) start() {

	// when channels are full due to backpressure, we write to "backlog"
	// and periodically flush postponed events directly to fsnotify results channel
	go r.flushBacklogLoop()

	// update usage statistics for watched files
	go r.usageCheckLoop()

	// periodically rescan all dir to pick files that are lost for some reason
	go r.periodicScanLoop()

	// process fsnotify events
	go r.mainLoop()
}

func (r *fsRouter) stop() {
	// mainLoop doesn't have side effects
	// so we need to stop only listeners
	// because they do clean old files
	// may cause "rm from fs"/"rm from offsets db" race
	r.cancelListeners()
	r.listenersWg.Wait()
}

func (r *fsRouter) mainLoop() {

	r.listenersCtx, r.cancelListeners = context.WithCancel(context.TODO())

	go func() {
		err := ReadDirIntoChannel(r.baseDir, r.watcher.Events, false)
		if err != nil {
			log.Errorf("initial base dir scan failed: %v", err)
		}
	}()

	for {
		select {
		case ev := <-r.watcher.Events:
			err := func() error {
				st, err := files.Lstat(ev.Name)
				if os.IsNotExist(err) {
					// no need to remove any watches when watched dir is deleted
					// fsnotify will do it internally before sending event to us
					delete(r.alreadyWatching, ev.Name)
					return nil
				}
				if err != nil {
					return fmt.Errorf("failed to stat %s: %v", ev.Name, err)
				}
				if st.IsLink() {
					// we can have symlinks in workdir running in router/relay mode
					// just ignore them
					return nil
				}
				if st.IsDir() {
					// possibly new directory
					// we need to add it's watch and perform initial scan
					// to prevent race between new watch and old files, that were already there
					return r.maybeStartWatching(ev.Name)
				}
				r.sendCategoryEvent(ev.Name, st.Inode(), uint64(st.Size()), false)
				return nil
			}()
			if err != nil {
				log.Errorf("error processing event %v: %v", ev, err)
			}
		case req := <-r.finalSyncCh:
			r.sendCategoryEvent(req.AbsolutePath, req.Inode, req.Size, true)
		case err := <-r.watcher.Errors:
			log.Errorf("fsnotify internal error: %v", err)
		}
	}
}

func (r *fsRouter) sendCategoryEvent(absolutePath string, inode, size uint64, isFinal bool) {

	event := category.NewEvent(
		ExtractCategoryName(r.baseDir, absolutePath),
		absolutePath,
		inode,
		size,
		r.config.GetAlwaysFlock(),
		isFinal,
	)

	listener, ok := r.listeners[event.Name]
	if !ok {
		listener = category.NewListener(
			r.listenersCtx,
			r.config,
			event.Name,
			r.offsetsDb,
			r.usageChecker,
			make(chan category.Event, r.config.GetFileEventsBufferSize()),
			r.netRouter.getOutChanForCategory(event.Name),
			r.finalSyncCh,
		)
		r.listeners[event.Name] = listener

		r.listenersWg.Add(1)
		go func() {
			listener.Listen()
			r.listenersWg.Done()
		}()
	}
	select {
	case listener.InCh <- event:
	default:
		r.backlog.add(event.AbsolutePath)
	}
}

func ExtractCategoryName(baseDir, absolutePath string) string {
	eventPath := strings.TrimPrefix(absolutePath, baseDir+string(os.PathSeparator))
	delimIdx := strings.IndexRune(eventPath, os.PathSeparator)
	if delimIdx == -1 {
		eventPath = strings.TrimSuffix(eventPath, category.OLD_FILE_SUFFIX)
		eventPath = strings.TrimSuffix(eventPath, category.LOG_FILE_SUFFIX)
		eventPath = strings.TrimSuffix(eventPath, category.BIG_FILE_SUFFIX)
		// order matters
		return eventPath
	}
	return eventPath[:delimIdx]
}

func (r *fsRouter) maybeStartWatching(absolutePath string) error {

	if strings.Count(strings.TrimPrefix(absolutePath, r.baseDir), string(os.PathSeparator)) > 1 { // single '/' is always first
		// do not enter sub-dirs
		return nil
	}

	// even if we are already watching dir, rescan it
	if !r.alreadyWatching[absolutePath] {
		err := r.watcher.Add(absolutePath)
		if err != nil {
			return fmt.Errorf("failed to start watching new dir %s: %v", absolutePath, err)
		}
		r.alreadyWatching[absolutePath] = true
	}

	go func() {
		scanErr := ReadDirIntoChannel(absolutePath, r.watcher.Events, false)
		if scanErr != nil {
			log.Errorf("initial new dir scan failed for %s: %v", absolutePath, scanErr)
		}
	}()
	return nil
}

func (r *fsRouter) flushBacklogLoop() {
	ticker := time.Tick(int2sec(r.config.GetBacklogFlushInterval()))
	for {
		<-ticker
		r.trafficManager.SetBacklogSize(r.backlog.size())
		for _, name := range r.backlog.flush() {
			log.Infof("Writing event for %s from backlog (we may have overload)", name)
			r.watcher.Events <- fsnotify.Event{Name: name, Op: fsnotify.Create}
		}
	}
}

func (r *fsRouter) usageCheckLoop() {
	ticker := time.Tick(int2sec(r.config.GetUsageCheckInterval()))
	for {
		<-ticker
		err := r.usageChecker.Update()
		if err != nil {
			log.Errorf("failed to check usage: %v", err)
		}
	}
}

func (r *fsRouter) periodicScanLoop() {
	ticker := time.Tick(int2sec(r.config.GetPeriodicScanInterval()))
	for {
		<-ticker
		err := ReadDirIntoChannel(r.baseDir, r.watcher.Events, true)
		if err != nil {
			log.Errorf("periodic base dir scan failed: %v", err)
		}
	}
}
