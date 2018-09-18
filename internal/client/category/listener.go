package category

import (
	"badoo/_packages/log"
	lsdProto "github.com/badoo/lsd/proto"
	"fmt"
	"os"
	"github.com/badoo/lsd/internal/client/offsets"
	"github.com/badoo/lsd/internal/client/files"
	"github.com/badoo/lsd/internal/client/usage"

	"context"

	"time"

	"bufio"

	"github.com/gogo/protobuf/proto"
)

func NewListener(ctx context.Context, config *lsdProto.LsdConfigClientConfigT, name string, odb *offsets.Db, checker *usage.Checker, inCh chan Event, outCh chan *lsdProto.RequestNewEventsEventT, finalSyncCh chan FinalSyncRequest) *Listener {
	return &Listener{
		ctx:                ctx,
		InCh:               inCh,
		finalSyncCh:        finalSyncCh,
		outCh:              outCh,
		name:               name,
		fileRotateInterval: time.Second * time.Duration(config.GetFileRotateInterval()),
		maxFileSize:        config.GetMaxFileSize(),
		files:              make(map[uint64]*fileInfo),
		// memory usage / allocations tradeoff
		// we always read only single category's file at any time
		// that's why we can use single buffer for all file reads
		// if we allocate it on each read, we consume a lot of CPU
		// if we create buffer for each fileInfo we consume a lot of memory
		readBuffer:   bufio.NewReaderSize(nil, READ_BUFFER_SIZE),
		cleanQueue:   make(map[string]cleanRequest),
		offsetsDb:    odb,
		usageChecker: checker,
	}
}

type Listener struct {
	ctx         context.Context
	InCh        chan Event
	outCh       chan *lsdProto.RequestNewEventsEventT
	finalSyncCh chan FinalSyncRequest // just a channel to request "final" sync for corrupted/orphan files

	name               string
	fileRotateInterval time.Duration
	maxFileSize        uint64
	files              map[uint64]*fileInfo

	readBuffer *bufio.Reader

	offsetsDb    *offsets.Db
	usageChecker *usage.Checker
	cleanQueue   map[string]cleanRequest // orig file name => cleanData with .old file name
}

func (cl *Listener) Listen() {

	// on first iteration we mark files to be rotated (half period)
	// on second we try rotate them - only files that exist at least half a period
	rotateTicker := time.Tick(cl.fileRotateInterval / 2)
	rotateQueue := make([]uint64, 0)
	for {
		select {
		case event := <-cl.InCh:
			// register file, send data and rotate if necessary
			err := cl.processNewEvent(event)
			if err == nil {
				break
			}
			if os.IsNotExist(err) && event.IsRotated {
				// file has been cleaned,
				// but we have already queued some events about it in fs router
				break
			}
			// an error occured - we need to close file handle
			// and clean all associated info
			// we will retry to do smth on next event
			log.Error(err)
			err = cl.forget(event.Inode)
			if err != nil {
				log.Errorf("failed to forget after send/rotate %v: %v", event, err)
			}
		case <-rotateTicker:

			for _, inode := range rotateQueue {

				fi, ok := cl.files[inode]
				if !ok {
					// file has been closed for some reason
					// don't mind
					continue
				}
				err := cl.rotate(inode, fi)
				if os.IsNotExist(err) {
					// original file is absent - nothing to do here
					// just forget about it
					cl.offsetsDb.Delete(inode)
					err = cl.forget(inode)
				}
				if err != nil {
					log.Errorf("failed to rotate %d (periodic): %v: %v", inode, fi, err)
				}
			}

			// fill rotate queue for new files
			// in order to rotate them on next iteration
			rotateQueue = rotateQueue[:0]
			for inode := range cl.files {
				rotateQueue = append(rotateQueue, inode)
			}
		case <-cl.usageChecker.UpdateCh:

			// try to clean all rotated files
			// iterate all and delete if ok
			for origName, request := range cl.cleanQueue {

				done, err := cl.maybeCleanFile(request)
				if err != nil {
					log.Errorf("error during old file clean %v: %v", request, err)
				}
				if !done {
					continue
				}
				cl.offsetsDb.Delete(request.inode)
				cl.usageChecker.Unwatch(request.inode)
				err = cl.forget(request.inode)
				if err != nil {
					log.Errorf("failed to forget %v after this file is cleaned: %v", request, err)
				}
				delete(cl.cleanQueue, origName)
			}
		case <-cl.ctx.Done():
			return
		}
	}
}

func (cl *Listener) processNewEvent(event Event) error {

	// check if we know about this file
	// open and register, if not
	info, err := cl.registerEvent(&event)
	if os.IsNotExist(err) {
		// this may not be an error in some cases
		// let's consider in parent code
		return err
	}
	if err != nil {
		return fmt.Errorf("failed to register incoming event %v: %v", event, err)
	}

	err = cl.sendChanges(info, event)
	if err != nil {
		return fmt.Errorf("failed to send changes for event: %v info: %v: %v\n closing file", event, info, err)
	}

	err = cl.maybeRotate(event, info)
	if err != nil {
		return fmt.Errorf("failed to rotate %v %v: %v", event, info, err)
	}
	return nil
}

func (cl *Listener) registerEvent(event *Event) (*fileInfo, error) {

	info, ok := cl.files[event.Inode]
	// already have this file opened
	if ok {
		return info, nil
	}

	fp, err := os.Open(event.AbsolutePath)
	if os.IsNotExist(err) {
		// this may not be an error in some cases
		// let's consider in parent code
		return info, err
	}
	if err != nil {
		return info, fmt.Errorf("failed to open file %s: %v", event.AbsolutePath, err)
	}
	shouldClose := true
	defer func() {
		if !shouldClose {
			return
		}
		err = fp.Close()
		if err != nil {
			log.Errorf("failed to close %s: %v", fp.Name(), err)
		}
	}()

	st, err := files.StatFp(fp)
	if err != nil {
		return info, fmt.Errorf("failed to get stat of %s: %v", event.AbsolutePath, err)
	}
	event.Inode = st.Inode()
	event.Size = uint64(st.Size())

	// we need to recheck event in map, because inode may change
	info, ok = cl.files[event.Inode]
	if ok {
		return info, nil
	}

	entry := cl.getValidOffsetDbEntry(event)
	newOffset, err := fp.Seek(int64(entry.Offset), 0)
	if err != nil {
		return info, fmt.Errorf("failed to seek to previous offset on file %s: %v", event.AbsolutePath, err)
	}
	shouldClose = false

	info = &fileInfo{
		actualPath: event.AbsolutePath,
		isRotated:  event.IsRotated,
		fp:         fp,
		lastOffset: uint64(newOffset),
		reader:     newEventReader(fp, event.IsExclusive),
	}
	cl.files[event.Inode] = info
	return info, nil
}

func (cl *Listener) getValidOffsetDbEntry(event *Event) offsets.DbEntry {

	entry, ok := cl.offsetsDb.Get(event.Inode)
	if !ok {
		// new file
		entry.Ino = event.Inode
		entry.LastFileName = event.AbsolutePath
		entry.Offset = 0
		cl.offsetsDb.Set(event.Inode, entry)
		return entry
	}
	if !entry.IsValid(event.AbsolutePath) {
		log.Warnf("Inode %d collision found for %s (expected to have %s)", event.Inode, event.AbsolutePath, entry.LastFileName)
		entry.Ino = event.Inode
		entry.Offset = 0
		entry.LastFileName = event.AbsolutePath
		cl.offsetsDb.Set(event.Inode, entry)
		return entry
	}
	if entry.LastFileName == "" {
		// backwards compatibility
		entry.LastFileName = event.AbsolutePath
		cl.offsetsDb.Set(event.Inode, entry)
	}
	return entry
}

func (cl *Listener) forget(inode uint64) error {

	info, ok := cl.files[inode]
	if !ok {
		return nil
	}
	delete(cl.files, inode)
	return info.fp.Close()
}

func (cl *Listener) sendChanges(info *fileInfo, event Event) error {

	if info.lastOffset == event.Size {
		return nil
	}

	// we always read file up to the end,
	// so we will not have anything buffered and then lost after leaving this function
	cl.readBuffer.Reset(info.reader.fileReader)
	for {
		lines, err := info.readLines(cl.readBuffer, event.IsFinal)
		if err != nil {
			return fmt.Errorf("failed to read lines: %v", err)
		}
		if len(lines) == 0 {
			break
		}
		req := &lsdProto.RequestNewEventsEventT{
			Category: proto.String(cl.name),
			Offset:   proto.Uint64(info.lastOffset),
			Inode:    proto.Uint64(event.Inode),
			Lines:    lines,
		}
		select {
		case cl.outCh <- req:
		case <-cl.ctx.Done():
			return nil
		}
	}
	return nil
}

func (cl *Listener) maybeRotate(event Event, info *fileInfo) error {

	// already rotated - just add to clean queue if we didn't yet
	if event.IsRotated {
		origPath := event.getOrigFilePath()
		if _, ok := cl.cleanQueue[origPath]; ok {
			// already in queue
			return nil
		}
		cl.addToCleanQueue(origPath, event.AbsolutePath, event.Inode)
		return nil
	}
	if event.Size > cl.maxFileSize {
		return cl.rotate(event.Inode, info)
	}
	return nil
}

func (cl *Listener) rotate(inode uint64, fi *fileInfo) error {

	if fi.isRotated {
		return nil
	}
	if _, ok := cl.cleanQueue[fi.actualPath]; ok {
		// rotate is already done for previous file
		// we have to wait for it to be sent and deleted before rotating new one
		return nil
	}

	newPath := fi.actualPath + OLD_FILE_SUFFIX
	st, err := files.Lstat(newPath)
	if err == nil {
		// rotate has been scheduled for previous file,
		// but we didn't known about it yet (after restart)
		// same as comment above
		cl.addToCleanQueue(fi.actualPath, newPath, st.Inode())
		return nil
	}
	if !os.IsNotExist(err) {
		return fmt.Errorf("failed to get stat for %s: %v", newPath, err)
	}

	err = os.Rename(fi.actualPath, newPath)
	// file is absent for some external reason
	// we need to process this error somehow in parent code
	if os.IsNotExist(err) {
		return err
	}
	if err != nil {
		return fmt.Errorf("failed to rename %s to %s during rotate: %v", fi.actualPath, newPath, err)
	}

	cl.addToCleanQueue(fi.actualPath, newPath, inode)
	fi.actualPath = newPath
	fi.isRotated = true
	return nil
}

func (cl *Listener) addToCleanQueue(origPath, newPath string, inode uint64) {
	cl.usageChecker.Watch(inode)
	cl.cleanQueue[origPath] = cleanRequest{absolutePath: newPath, inode: inode}
}

func (cl *Listener) maybeCleanFile(request cleanRequest) (bool, error) {

	if !cl.usageChecker.IsFree(request.inode) {
		// wait for file to become free
		return false, nil
	}
	st, err := files.Lstat(request.absolutePath)
	if os.IsNotExist(err) {
		// file has already absent - nothing to do
		return true, nil
	}
	if err != nil {
		return false, fmt.Errorf("failed to get stat for %s (during clean): %v", request.absolutePath, err)
	}
	if st.Inode() != request.inode {
		// this may happen if someone delete files manually
		// just forget about previous (missing) inode
		// and requeue clean with new one
		return true, fmt.Errorf("inode collision found for %v. actual inode is %d", request, st.Inode())
	}
	entry, _ := cl.offsetsDb.Get(request.inode)
	if uint64(st.Size()) > entry.Offset {
		log.Infof("file %s is not fully synced %d/%d", request.absolutePath, st.Size(), entry.Offset)
		select {
		case cl.finalSyncCh <- FinalSyncRequest{
			AbsolutePath: request.absolutePath,
			Inode:        st.Inode(),
			Size:         uint64(st.Size()),
		}:
		default:
			// failed to request final sync? not a problem, let's try next time
		}
		return false, nil
	}
	err = os.Remove(request.absolutePath)
	if err != nil && !os.IsNotExist(err) {
		return false, fmt.Errorf("failed to delete %s: %v", request.absolutePath, err)
	}
	return true, nil
}
