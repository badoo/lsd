package client

import (
	"badoo/_packages/gpbrpc"
	"badoo/_packages/log"
	"github.com/badoo/lsd/internal/common"
	"bufio"
	"encoding/json"
	"expvar"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/gogo/protobuf/proto"
	"gopkg.in/fsnotify.v1"
	"github.com/badoo/lsd/proto"
)

type (
	rotateEvent struct {
		oldName string
		newName string
	}

	// entry in offsets.db, so field names must be capitalized for JSON extension to be able to marshal/unmarshal
	offsetsDbEntry struct {
		Ino uint64
		Off int64
	}

	deleteRequest struct {
		filename string
		ino      uint64
	}

	// Check if file with inode ino is open by someone. Write boolean response into res when request is complete
	usageRequest struct {
		ino    uint64
		isFree chan bool
	}

	receiver struct {
		events  chan *lsd.RequestNewEventsEventT
		addr    string
		offline bool
	}

	receiverGroup struct {
		categories      []*regexp.Regexp
		receivers       []*receiver
		curReceiverIdx  int
		prefixSharding  bool
		cutPrefix       bool
		prefixDelimiter string
		isDefaultRoute  bool
		outChan         chan *lsd.RequestNewEventsEventT
		replyChan       chan *lsd.ResponseOffsetsOffsetT
		offlineChan     chan *offlineEvent
	}

	outEntry struct {
		offset           uint64
		pendingFragments uint64 // number of chunks that need to be received for the specified offset to consider is delivered
	}

	offlineEvent struct {
		idx     int // index in receivers array
		offline bool
		respCh  chan bool
	}

	fsEvent struct {
		isFinal  bool // whether or not event is final (sent before file deletion to force delivery even if file does not end with EOF)
		filename string
		ino      uint64
	}
)

const (
	OLD_FILE_SUFFIX = ".old"
	BIG_FILE_SUFFIX = "_big"
	LOG_FILE_SUFFIX = ".log"

	MAX_ROTATE_TRIES   = 100000
	ROTATE_RETRY_SLEEP = 10 * time.Second

	MAX_DELETE_TRIES   = 100000
	DELETE_RETRY_SLEEP = 10 * time.Second

	MAX_BYTES_PER_MESSAGE = 1 << 16

	CONN_TIMEOUT    = time.Second
	REQUEST_TIMEOUT = 30 * time.Second

	TRAFFIC_SPEED_CALC_INTERVAL = 10 // seconds

	MAX_BUSY_REPORT_INTERVAL = 60 * time.Second

	MAX_MESSAGES_PER_BATCH            = 50 // if batches are big (e.g. 64kb) then too many batches could cause OOM eventually
	MAX_PREFIX_SHARDING_AMPLIFICATION = 10
	MAX_BYTES_PER_BATCH               = 1 << 20

	MAX_IO_THREADS     = 16 // how many threads can be spawned for IO operations. TODO: make it configurable
	MAX_DELETE_THREADS = 10000

	DIRECTORY_REMOVED_MARKER = ""
)

var (
	ioRequestChan = make(chan chan bool) // channel for getting "ticket" for new concurrent io operation
	ioFreeChan    = make(chan bool, 10)  // channel for freeing io operation

	deleteIoRequestChan = make(chan chan bool)
	deleteIoFreeChan    = make(chan bool, 10)

	// acknowledged offsets per inode
	offsetsMutex = new(sync.Mutex)
	offsets      = make(map[uint64]int64) // inode => offset

	// file read positions per inode
	localOffsetsMutex = new(sync.Mutex)
	localOffsets      = make(map[uint64]int64) // inode => offset

	config *lsd.LsdConfigClientConfigT

	fsNotifyChan      = make(chan fsEvent, 1000)
	rotateEvChan      = make(chan *rotateEvent, 10)
	usageRequestsChan = make(chan *usageRequest, 10)

	// per-category out traffic stats
	categoryOutTrafficMutex = new(sync.Mutex)
	categoryOutTraffic      = make(map[string]int64)

	// per-category out traffic speed stats
	categoryOutTrafficSpeedMutex = new(sync.Mutex)
	categoryOutTrafficSpeed      = make(map[string]int64)

	// currently deleting files index so that we do not try to delete files twice
	currentlyDeletingMutex = new(sync.Mutex)
	currentlyDeleting      = make(map[uint64]bool)

	// parsed client config
	receiverGroupsMutex = new(sync.Mutex)
	receiverGroups      = make([]*receiverGroup, 0)
)

func sendDirEvents(dir string, events chan<- fsEvent, dirEvents chan<- fsnotify.Event) {
	// After we started watching for a directory, we need to process all files once
	fp, err := os.Open(dir)
	if err != nil {
		log.Errorf("Could not open source directory: %s", err.Error())
		return
	}
	defer fp.Close()

	for {
		fis, err := fp.Readdir(100)
		if err == nil {
			for _, fi := range fis {
				if !fi.IsDir() {
					events <- fsEvent{filename: fi.Name()}
				} else {
					dirEvents <- fsnotify.Event{Name: fi.Name()}
				}
			}
		} else {
			if err != io.EOF {
				log.Errorf("Could not read directory names: %s", err.Error())
			}
			return
		}
	}
}

func streamNewCategory(dirpath, category string, watcher *fsnotify.Watcher, watchedDirs map[string]chan fsEvent) {
	if watchedDirs[dirpath] != nil {
		return
	}

	log.Infof("Streaming new category: %s from %s", category, dirpath)

	watcher.Add(dirpath)
	dirCh := make(chan fsEvent, 10)
	watchedDirs[dirpath] = dirCh

	go streamLogsChunked(category, dirpath, dirCh)
}

// goroutine that starts to watch directory "dir" and sends file names of changed files to "events" chan
func runWatcher(dir string, events chan<- fsEvent) {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Fatalln("Could not set file system watcher: " + err.Error())
	}
	defer watcher.Close()

	dir = strings.TrimSuffix(dir, string(os.PathSeparator))

	if err = watcher.Add(dir); err != nil {
		log.Fatalln("Could not start watching source directory: " + err.Error())
	}

	watchedDirs := make(map[string]chan fsEvent) // dir => chan filename

	go sendDirEvents(dir, events, watcher.Events)

	for {
		select {
		case ev := <-watcher.Events:
			var evFilePath string

			if filepath.IsAbs(ev.Name) {
				evFilePath = ev.Name
			} else {
				evFilePath = dir + string(os.PathSeparator) + ev.Name
			}

			st, err := os.Stat(evFilePath)

			if err == nil {
				filename := strings.TrimPrefix(strings.TrimPrefix(evFilePath, dir), string(os.PathSeparator))

				if st.IsDir() {
					// do not enter sub-dirs
					if strings.Count(filename, string(os.PathSeparator)) == 0 {
						streamNewCategory(evFilePath, filepath.Base(evFilePath), watcher, watchedDirs)
					}
				} else {
					if !strings.Contains(filename, string(os.PathSeparator)) { // plain file like "category.log" or "category_big.log"
						events <- fsEvent{filename: filename}
					} else { // category file like "category/something.log"
						parts := strings.Split(filename, string(os.PathSeparator))
						category := parts[0]
						dirPath := dir + string(os.PathSeparator) + category

						// it is entirely possible that event about directory was lost or something
						// we do not want to panic or block forever on this occasion
						if watchedDirs[dirPath] == nil {
							log.Info("Received event about file without receiving event about directory first for " + dirPath)
							streamNewCategory(dirPath, category, watcher, watchedDirs)
						} else {
							watchedDirs[dirPath] <- fsEvent{filename: parts[1]}
						}
					}
				}
			} else if os.IsNotExist(err) && watchedDirs[evFilePath] != nil {
				watcher.Remove(evFilePath)
				watchedDirs[evFilePath] <- fsEvent{filename: DIRECTORY_REMOVED_MARKER}
				delete(watchedDirs, evFilePath)
			}
		case err = <-watcher.Errors:
			log.Errorf("Watch error occured: %s", err.Error())
		}
	}
}

// send changes that occured in file, but not more than MAX_LINES_PER_BATCH
// if fp is not nil, then unlock it
// if too many lines need to be read, then event about filename will be sent to externEvents
func sendChanges(category string, ev fsEvent, fp *os.File, ino uint64, offset *int64, externEvents chan<- fsEvent, outCh chan<- *lsd.RequestNewEventsEventT, needUnlock bool) error {
	common.RequestIO(ioRequestChan)
	defer common.ReleaseIO(ioFreeChan)

	r := bufio.NewReader(fp)
	newOffset := *offset

	var bytesRead int64
	var lines []string

	for {
		str, err := r.ReadString('\n')
		if err == io.EOF {
			// We did partially read line and hit EOF.
			// In this case we need to discard read data with the exception of very rare case when we want to delete
			// file that is no longer used and it is corrupted (does not have EOL marker at the end of file).
			// Seek is required in this case because we need to set read position right at the beginning of line in order
			// to stream the line fully next time

			if len(str) > 0 {
				if ev.isFinal && ino == ev.ino {
					log.Errorf("Partially read line at EOF in file %s: '%s'", ev.filename, str)
				} else {
					break
				}
			} else if len(str) == 0 {
				break
			}
		} else if err != nil {
			log.Errorf("Could not read file: %s", err.Error())
			return err
		}

		lines = append(lines, str)
		bytesRead += int64(len(str))

		if bytesRead >= MAX_BYTES_PER_MESSAGE {
			externEvents <- fsEvent{filename: filepath.Base(ev.filename), isFinal: ev.isFinal, ino: ev.ino} // trigger event again so we would read the rest
			break
		}

		if err == io.EOF {
			break
		}
	}

	if needUnlock {
		log.Debugf("Removing lock for %s in category %s", ev.filename, category)
		syscall.Flock(int(fp.Fd()), syscall.LOCK_UN)
	}

	newOffset += bytesRead

	if len(lines) > 0 {
		outCh <- &lsd.RequestNewEventsEventT{
			Category: proto.String(category),
			Offset:   proto.Uint64(uint64(newOffset)),
			Inode:    proto.Uint64(ino),
			Lines:    lines,
		}
	}

	*offset = newOffset

	fp.Seek(newOffset, os.SEEK_SET)
	return nil
}

func getOff(ino uint64) int64 {
	offsetsMutex.Lock()
	defer offsetsMutex.Unlock()
	return offsets[ino]
}

func deleteOff(ino uint64) {
	offsetsMutex.Lock()
	defer offsetsMutex.Unlock()

	delete(offsets, ino)
}

// read sorted filenames from dirpath into ch
// if dir read needs to be cancelled, send event to cancelCh
func readDirIntoChan(dirpath string, ch chan<- fsEvent, cancelCh <-chan bool) {
	dh, err := os.Open(dirpath)
	if err != nil {
		log.Errorf("Could not open (readDirIntoChan) %s: %s", dirpath, err.Error())
		return
	}
	defer dh.Close()

	filenames := make([]string, 0)

	for {
		fis, err := dh.Readdir(100)

		if fis != nil {
			for _, fi := range fis {
				if fi.IsDir() {
					continue
				}

				filenames = append(filenames, fi.Name())
			}
		}

		if err != nil {
			if err == io.EOF {
				break
			} else {
				log.Errorf("Could not read dir names from %s: %s", dirpath, err.Error())
				return
			}
		}
	}

	sort.Strings(filenames)
	for _, name := range filenames {
		select {
		case ch <- fsEvent{filename: name}:
		case <-cancelCh:
			return
		}
	}
}

// A separate goroutine for buffering events from filesystem for a specified category:
// events can be delivered much slower than triggered, so goroutines need a mechanism to prevent blocking.
// This goroutine buffers events from externEvents and resends them to events channel
// it also sends cancel event if it is received from externEvents
func categoryEvBufferer(category string, categoryEvents chan<- fsEvent, cancelCh chan<- bool, externEvents <-chan fsEvent) {
	evList := make([]fsEvent, 0)    // queue for events that have not yet been delivered
	evMap := make(map[fsEvent]bool) // map to filter duplicate events
	var events chan<- fsEvent       // send to nil map blocks forever, this allows to "turn off" select cases
	curEl := fsEvent{}

	defer func() {
		exitTimer := time.After(time.Minute) // ugly hack to make sure that all events are drained

		for {
			select {
			case <-externEvents:
				log.Warnf("Dropped event about category %s", category)
			case <-exitTimer:
				return
			}
		}
	}()

	for {
		select {
		case events <- curEl:
			delete(evMap, curEl)

			if len(evList) == 0 {
				curEl = fsEvent{}
				events = nil
			} else {
				curEl = evList[0]
				evList = evList[1:]
			}
		case newEl := <-externEvents:
			if newEl.filename == DIRECTORY_REMOVED_MARKER {
				log.Printf("Got a directory removed marker for category %s", category)
				select {
				case cancelCh <- true:
				default:
					log.Warnf("Could not send cancel event for category %s", category)
				}

				close(categoryEvents)
				return
			}

			if _, ok := evMap[newEl]; !ok {
				evMap[newEl] = true

				if events == nil {
					curEl = newEl
					events = categoryEvents
				} else {
					evList = append(evList, newEl)
				}
			}
		}
	}
}

// whether or not we can delete file now, or ".old" file is already present
func requestDelete(filename string, externEvents chan<- fsEvent) bool {
	_, err := os.Lstat(filename)
	if err != nil {
		if os.IsNotExist(err) {
			log.Debugf("Requested to delete non-existent file, skipping: %s", filename)
			return true
		}

		log.Errorf("Could not lstat file, not deleting it: %s: %s", filename, err.Error())
		return false
	}

	newFilename := strings.TrimSuffix(filename, OLD_FILE_SUFFIX) + OLD_FILE_SUFFIX
	if newFilename != filename {
		if _, err = os.Lstat(newFilename); err == nil {
			log.Debugf("File %s already exists, cannot perform renaming at the moment", newFilename)
			return false
		}

		performIo(func() { err = os.Rename(filename, newFilename) })
		if err != nil {
			log.Errorf("Could not rename %s -> %s, reason: %s", filename, newFilename, err.Error())
			return false
		}
	}

	common.RequestIO(deleteIoRequestChan)

	go func() {
		defer common.ReleaseIO(deleteIoFreeChan)
		deleteSync(newFilename, externEvents)
	}()

	return true
}

// Function checks whether or not file with the specified inode is currently used. Function execution may take a while
func fileIsFree(ino uint64) bool {
	answerCh := make(chan bool, 1)
	log.Debugf("Requested to check if file is used: inode #%d", ino)
	usageRequestsChan <- &usageRequest{ino: ino, isFree: answerCh}
	return <-answerCh
}

func performIo(cb func()) {
	common.RequestIO(ioRequestChan)
	defer common.ReleaseIO(ioFreeChan)

	cb()
}

func deleteSync(filename string, externEvents chan<- fsEvent) {
	var fi os.FileInfo
	var err error

	performIo(func() { fi, err = os.Stat(filename) })

	if err != nil {
		if !os.IsNotExist(err) {
			log.Errorf("Could not do stat (deleteSync) on file %s: %s", filename, err.Error())
		}
		return
	}

	ino := fi.Sys().(*syscall.Stat_t).Ino

	currentlyDeletingMutex.Lock()
	if currentlyDeleting[ino] {
		currentlyDeletingMutex.Unlock()
		log.Debugf("File %s with inode #%d is already being deleted, skipping", filename, ino)
		return
	}
	currentlyDeleting[ino] = true
	currentlyDeletingMutex.Unlock()

	log.Infof("Requested to delete %s with inode #%d", filename, ino)

	defer func() {
		currentlyDeletingMutex.Lock()
		delete(currentlyDeleting, ino)
		currentlyDeletingMutex.Unlock()
	}()

	tries := 0

	time.Sleep(time.Second) // give a second to deliver the file before trying to delete it

	for {
		if !fileIsFree(ino) {
			log.Debugf("File %s with inode %d is still used, cannot delete", filename, ino)
			time.Sleep(DELETE_RETRY_SLEEP)
			continue
		}

		readOff := getOff(ino)

		performIo(func() { fi, err = os.Stat(filename) })

		if err != nil {
			log.Errorf("Could not stat (deleteSync) %s: %s", filename, err)
			return
		}

		fileSz := fi.Size()

		if readOff >= fileSz {
			break
		}

		log.Printf("File %s with inode #%d is not fully sync: readoff=%d filesz=%d", filename, ino, readOff, fileSz)

		if tries >= MAX_DELETE_TRIES {
			log.Errorf("Could not wait for %s to be transferred, not deleting", filename)
			return
		}

		externEvents <- fsEvent{isFinal: true, filename: filepath.Base(filename), ino: ino}

		tries++
		time.Sleep(DELETE_RETRY_SLEEP)
	}

	performIo(func() { err = os.Remove(filename) })

	if err != nil {
		log.Errorf("Could not delete %s: %s", filename, err.Error())
		return
	}

	deleteOff(ino)

	func() {
		localOffsetsMutex.Lock()
		defer localOffsetsMutex.Unlock()

		delete(localOffsets, ino)
	}()
}

// goroutine that periodically checks which files are used by any processes and returns response for pending requests
func processUsageRequests() {
	requests := make([]*usageRequest, 0)
	checkInterv := time.Duration(config.GetUsageCheckInterval()) * time.Second
	tick := time.Tick(checkInterv)

	for {
		select {
		case <-tick:
			if len(requests) == 0 {
				break
			}

			inoMap := make(map[uint64]bool)
			for _, req := range requests {
				inoMap[req.ino] = true
			}

			freeInoMap, err := getFreeFiles(inoMap)
			if err != nil {
				log.Errorf("Could not check file usage: %s", err.Error())
				break
			}

			// throttle speed with which replies are sent so that we do not cause "stat" and "unlink" flood
			go func(requests []*usageRequest) {
				throttleInterv := checkInterv / time.Duration(len(requests)+1)

				for _, req := range requests {
					req.isFree <- freeInoMap[req.ino]
					time.Sleep(throttleInterv)
				}
			}(requests)

			requests = make([]*usageRequest, 0)
		case req := <-usageRequestsChan:
			requests = append(requests, req)
		}
	}
}

func clearPrevFiles(prevFiles map[string]string, latestEl string, externEvents chan<- fsEvent) {
	for fpath, filenameClean := range prevFiles {
		if latestEl == filenameClean {
			continue
		}

		if requestDelete(fpath, externEvents) {
			delete(prevFiles, fpath)
		}
	}
}

// Get output channel for specified category. Should be only called once per new category for performance reasons
func getOutChanForCategory(category string) chan *lsd.RequestNewEventsEventT {
	receiverGroupsMutex.Lock()
	defer receiverGroupsMutex.Unlock()

	var defaultCh chan *lsd.RequestNewEventsEventT

	for _, rg := range receiverGroups {
		if rg.isDefaultRoute {
			defaultCh = rg.outChan
			continue
		}

		for _, regex := range rg.categories {
			if regex.MatchString(category) {
				return rg.outChan
			}
		}
	}

	return defaultCh
}

// stream directory where files are written sequentially, as big chunked files
func streamLogsChunked(category, dirpath string, externEvents chan fsEvent) {
	outCh := getOutChanForCategory(category)
	if outCh == nil {
		log.Errorf("CRITICAL: Nowhere to stream category %s", category)
		return
	}

	events := make(chan fsEvent, 1)
	cancelCh := make(chan bool, 1)

	go readDirIntoChan(dirpath, events, cancelCh)
	go categoryEvBufferer(category, events, cancelCh, externEvents)

	latestEl := ""
	curFpath := ""

	curOffset := int64(0)

	var curFp *os.File
	var curIno uint64
	var filenameClean string

	prevFiles := make(map[string]string) // filepath => cleanFilename

	for ev := range events {
		filename := ev.filename
		fpath := dirpath + string(os.PathSeparator) + filename

		filenameClean = strings.TrimSuffix(filename, OLD_FILE_SUFFIX)

		// we must not delete _big file with the same name, so we remember "clean" filename without _big suffix
		if strings.HasSuffix(filenameClean, LOG_FILE_SUFFIX) {
			filenameClean = strings.TrimSuffix(strings.TrimSuffix(filenameClean, LOG_FILE_SUFFIX), BIG_FILE_SUFFIX) + LOG_FILE_SUFFIX
		} else {
			filenameClean = strings.TrimSuffix(filenameClean, BIG_FILE_SUFFIX)
		}

		if curFpath != fpath {
			log.Debugf("Going to open %s for streaming", fpath)

			var fi os.FileInfo
			var err error

			performIo(func() { fi, err = os.Lstat(fpath) })

			if err != nil {
				if !os.IsNotExist(err) {
					log.Errorf("Could not lstat(%s): %s, skipping file", fpath, err.Error())
				} else {
					log.Debugf("Could not find file %s, skipping", fpath)
				}
				continue
			}

			if !fi.Mode().IsRegular() {
				log.Debugf("File %s is not regular file, skipping", fpath)
				continue
			}

			var fp *os.File

			performIo(func() { fp, err = os.Open(fpath) })

			if err != nil {
				log.Errorf("Could not open (streamLogsChunked) %s: %s, skipping file", fpath, err.Error())
				continue
			}

			if curFp != nil {
				performIo(func() { curFp.Close() })
			}

			if latestEl == "" {
				latestEl = filenameClean
			} else if filenameClean > latestEl {
				latestEl = filenameClean
			}

			clearPrevFiles(prevFiles, latestEl, externEvents)

			curIno = getIno(fi)
			curFpath = fpath
			prevFiles[curFpath] = filenameClean
			curFp = fp

			localOffsetsMutex.Lock()
			curOffset = localOffsets[curIno]
			localOffsetsMutex.Unlock()

			if curOffset == 0 {
				offsetsMutex.Lock()
				curOffset = offsets[curIno]
				offsetsMutex.Unlock()
			}

			stillExists := true

			// ignore error and continue because we already opened the file and got the offset
			performIo(func() {
				if _, lerr := os.Lstat(fpath); lerr != nil && os.IsNotExist(lerr) {
					stillExists = false
				}

				if _, err = fp.Seek(curOffset, os.SEEK_SET); err != nil {
					log.Errorf("Could not seek to %d in file %s: %s", curOffset, curFpath, err.Error())
				}
			})

			// a bit ugly hack to prevent race between delete thread and streamer function
			// in rare cases file can be deleted after we successfully opened it, so we need to ensure that file
			// still exists
			if !stillExists && strings.HasSuffix(filename, OLD_FILE_SUFFIX) {
				log.Infof("File %s with inode #%d no longer exists, restoring current state", fpath, curIno)
				delete(prevFiles, curFpath)
				curIno = 0
				curOffset = 0
				curFpath = ""
				curFp = nil
				fp.Close()
				continue
			}
		}

		func() {
			needUnlock := false

			if strings.HasSuffix(clearFileName(fpath), BIG_FILE_SUFFIX) || config.GetAlwaysFlock() {
				log.Debugf("Trying to get lock for %s", fpath)
				if err := syscall.Flock(int(curFp.Fd()), syscall.LOCK_SH); err != nil {
					log.Errorf("Could not flock %s: %s, skipping file", fpath, err.Error())
					return
				}

				needUnlock = true
			}

			if err := sendChanges(category, ev, curFp, curIno, &curOffset, externEvents, outCh, needUnlock); err != nil {
				log.Errorf("Could not send changes: %s", err.Error())

				performIo(func() {
					if needUnlock {
						syscall.Flock(int(curFp.Fd()), syscall.LOCK_UN)
					}

					curFp.Seek(curOffset, os.SEEK_SET)
				})
				return
			}

			localOffsetsMutex.Lock()
			localOffsets[curIno] = curOffset
			localOffsetsMutex.Unlock()
		}()
	}
}

func getIno(fi os.FileInfo) uint64 {
	return fi.Sys().(*syscall.Stat_t).Ino
}

func clearFileName(filename string) string {
	return strings.TrimSuffix(strings.TrimSuffix(filename, OLD_FILE_SUFFIX), LOG_FILE_SUFFIX)
}

func streamLogsSimple(filename string, chEvs chan fsEvent, rotEvs chan *rotateEvent) {
	category := clearFileName(filename)
	isBig := false
	if strings.HasSuffix(category, BIG_FILE_SUFFIX) {
		category = strings.TrimSuffix(category, BIG_FILE_SUFFIX)
		isBig = true
	}

	outCh := getOutChanForCategory(category)
	if outCh == nil {
		log.Errorf("CRITICAL: Nowhere to stream category %s", category)
		return
	}

	log.Infof("Streaming for category %s, filename = %s", category, filename)

	canRotate := !strings.HasSuffix(filename, OLD_FILE_SUFFIX)
	requestedRotate := false

	var fp *os.File
	var err error

	dirPrefix := config.GetSourceDir() + string(os.PathSeparator)

	performIo(func() { fp, err = os.Open(config.GetSourceDir() + string(os.PathSeparator) + filename) })

	if err != nil {
		if !os.IsNotExist(err) {
			log.Errorln("Could not open (streamLogsSimple) ", filename, ":", err.Error())
		}
		log.Debugln("No such file: " + filename)
		return
	}
	defer performIo(func() { fp.Close() })

	var fi os.FileInfo

	performIo(func() { fi, err = fp.Stat() })

	if err != nil {
		log.Errorln("Could not stat ", filename, ": ", err.Error())
		return
	}

	ino := getIno(fi)

	offsetsMutex.Lock()
	offset := offsets[ino]
	offsetsMutex.Unlock()

	performIo(func() { _, err = fp.Seek(offset, os.SEEK_SET) })

	if err != nil {
		log.Errorln("Could not seek to ", offset, ": ", err.Error())
		return
	}

	log.Infoln("Listening events for " + filename)
	periodicExistenceCheckCh := time.After(time.Minute)

	for {
		select {
		case ev := <-chEvs:
			log.Debugln("File changed: ", filename)

			needUnlock := false

			// Shared lock needs to be taken for big files
			// We also need to release lock after we read all required contents, not after we sent them successfully
			// That is why we need to unlock file pointer in sendChanges function before sending data

			if isBig {
				log.Debugln("Trying to get shared lock for ", filename)
				err = syscall.Flock(int(fp.Fd()), syscall.LOCK_SH)
				if err != nil {
					log.Errorln("Could not set flock(LOCK_SH) for file ", fp.Name())
					return
				}
				needUnlock = true
			}

			if err := sendChanges(category, ev, fp, ino, &offset, fsNotifyChan, outCh, needUnlock); err != nil {
				log.Errorln("Stopping to stream ", filename, ", error ocurred: ", err.Error())
				return
			}

			if offset >= int64(config.GetMaxFileSize()) && canRotate && !requestedRotate {
				requestedRotate = true
				go requestRotate(filename)
			}
		case <-periodicExistenceCheckCh:
			func() {
				common.RequestIO(ioRequestChan)
				defer common.ReleaseIO(ioFreeChan)

				_, err = os.Stat(dirPrefix + filename)
				if err != nil && !strings.HasSuffix(filename, OLD_FILE_SUFFIX) {
					_, err = os.Stat(dirPrefix + filename + OLD_FILE_SUFFIX)
				}
			}()

			if err != nil {
				if os.IsNotExist(err) {
					log.Printf("File %s was externally deleted, stopping to listen for events", filename)
					deleteOff(ino)
					return
				} else {
					log.Printf("Could not stat file %s: %s", filename, err.Error())
				}
			}

			periodicExistenceCheckCh = time.After(time.Minute)
		case ev := <-rotEvs:
			if ev.oldName == filename {
				// Some events could be lost or delivered in wrong order during rotate, so force re-processing
				log.Println("Received event about rotated " + filename + ", updating filename to " + ev.newName)
				filename = ev.newName
				canRotate = false
				fsNotifyChan <- fsEvent{filename: filename}
				break
			} else {
				// we already were listening the rotated file; now it is deleted, so stop listening
				log.Println("Stopping to listen " + filename)
				return
			}
		}
	}
}

func requestRotate(filename string) {
	var fp *os.File
	var err error

	performIo(func() {
		fp, err = os.OpenFile(config.GetSourceDir()+string(os.PathSeparator)+filename+OLD_FILE_SUFFIX, os.O_APPEND|os.O_WRONLY, 0666)
	})
	if err != nil {
		if os.IsNotExist(err) {
			doRotate(filename, 0)
		} else {
			log.Println("Could not open (requestRotate) ", filename, ":", err.Error())
		}
		return
	}
	defer fp.Close()

	fi, err := fp.Stat()
	if err != nil {
		log.Println("Could not do fstat on file " + fp.Name() + ": " + err.Error())
		return
	}

	ino := fi.Sys().(*syscall.Stat_t).Ino

	tries := 0

	for {
		if !fileIsFree(ino) {
			log.Debugf("File %s with inode %d is still used, cannot rotate", filename, ino)
			time.Sleep(ROTATE_RETRY_SLEEP)
			continue
		}

		readOff := getOff(ino)
		fileSz, err := fp.Seek(0, os.SEEK_END)
		if err != nil {
			log.Println("Could not seek " + fp.Name() + " until end: " + err.Error())
			return
		}

		if readOff >= fileSz {
			break
		}

		log.Println("File ", filename, " is not fully sync: readoff=", readOff, ", filesz=", fileSz)

		fsNotifyChan <- fsEvent{isFinal: true, filename: filename, ino: ino}

		if tries >= MAX_ROTATE_TRIES {
			log.Errorln("Could not wait for " + fp.Name() + " to be transferred, not rotating")
			return
		}

		tries++
		time.Sleep(ROTATE_RETRY_SLEEP)
	}

	doRotate(filename, ino)
}

func doRotate(filename string, ino uint64) {
	log.Println("Rotating " + filename)

	fpath := config.GetSourceDir() + string(os.PathSeparator) + filename

	var err error
	performIo(func() { err = os.Rename(fpath, fpath+OLD_FILE_SUFFIX) })
	if err != nil {
		log.Errorln("Could not rotate file " + filename + ": " + err.Error())
		return
	}

	deleteOff(ino)

	rotateEvChan <- &rotateEvent{oldName: filename, newName: filename + OLD_FILE_SUFFIX}
}

// goroutine that handles fsnotify and rotate events so that file descriptors are used properly
func fsnotifyRouter() {
	chEvs := make(map[string]chan fsEvent)
	evNames := make(map[chan fsEvent]string)
	rotEvs := make(map[chan fsEvent]chan *rotateEvent)
	unsub := make(chan chan fsEvent, 1)

	for {
		select {
		case ev := <-rotateEvChan:
			// Upon rotate, we need to send event to two streamers:
			//   1. Current file streamer
			//   2. "old" file streamer - the file that deleted during rotate

			ch, ok := chEvs[ev.oldName]
			if !ok {
				break
			}

			evNames[ch] = ev.newName

			if oldCh, ok := chEvs[ev.newName]; ok {
				select {
				case rotEvs[oldCh] <- ev:
				default:
				}
			}

			chEvs[ev.newName] = ch
			delete(chEvs, ev.oldName)

			select {
			case rotEvs[ch] <- ev:
			default:
			}
		case ev := <-fsNotifyChan:
			name := ev.filename
			log.Debugln("got event about " + name)

			ch, ok := chEvs[name]
			if !ok {
				ch = make(chan fsEvent, 1)
				chEvs[name] = ch
				evNames[ch] = name

				// at most 2 rotate events can happen - first is rename to .old, second is delete of .old file
				rotCh := make(chan *rotateEvent, 2)
				rotEvs[ch] = rotCh
				go func(name string, ch chan fsEvent, rotCh chan *rotateEvent) {
					streamLogsSimple(name, ch, rotCh)
					unsub <- ch
				}(name, ch, rotCh)
			}

			log.Debugln("channel ", ch)
			log.Debugln("sending event about " + name)
			select {
			case ch <- ev:
			default:
			}

			log.Debugln("sent event about " + name)
		case ch := <-unsub:
			if name, ok := evNames[ch]; ok {
				log.Debugln("Unsubscribing " + name)
				delete(evNames, ch)
				delete(rotEvs, ch)
				if chEvs[name] == ch {
					delete(chEvs, name)
				}
			}
		}
	}
}

// get all inodes from dir and write them into result (inode => true)
func getAllInodes(dir string, result map[uint64]bool) error {
	fp, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer fp.Close()

	var fis []os.FileInfo
	for {
		fis, err = fp.Readdir(100)
		if err != nil {
			if err == io.EOF {
				break
			} else {
				return err
			}
		}

		for _, fi := range fis {
			if fi.IsDir() {
				err = getAllInodes(dir+string(os.PathSeparator)+fi.Name(), result)
				if err != nil {
					return err
				}
			} else {
				result[fi.Sys().(*syscall.Stat_t).Ino] = true
			}
		}
	}

	return nil
}

func fillOffsets(contents []byte) {
	offsetsMutex.Lock()
	defer offsetsMutex.Unlock()

	offsets = make(map[uint64]int64)
	jsonDb := make([]*offsetsDbEntry, 0)
	err := json.Unmarshal(contents, &jsonDb)
	if err != nil {
		log.Println("Could not unmarshal offsets db: ", err.Error())
	} else {
		for _, el := range jsonDb {
			offsets[el.Ino] = el.Off
		}
	}
}

func clearMissingInodes(allInodes map[uint64]bool) {
	offsetsMutex.Lock()
	defer offsetsMutex.Unlock()

	for ino := range offsets {
		if _, ok := allInodes[ino]; !ok {
			delete(offsets, ino)
		}
	}
}

func readOffsets() {
	log.Info("Reading offsets db")
	fp, err := os.Open(config.GetOffsetsDb())
	if err == nil {
		contents, err := ioutil.ReadAll(fp)
		fp.Close()

		if err == nil {
			fillOffsets(contents)
		} else {
			log.Info("Could not read offsets db: ", err.Error())
		}

		allInodes := make(map[uint64]bool)
		err = getAllInodes(config.GetSourceDir(), allInodes)
		if err != nil {
			log.Panic("Could not get all inodes from ", config.GetSourceDir(), ": ", err.Error())
		}

		clearMissingInodes(allInodes)
	}
}

func saveOffsets() {
	fp, err := os.OpenFile(config.GetOffsetsDb()+".tmp", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	if err != nil {
		log.Fatalln("Could not create tmp file for saving offsets: ", err.Error())
	}

	offsetsMutex.Lock()
	jsonDb := make([]*offsetsDbEntry, 0)
	for ino, off := range offsets {
		jsonDb = append(jsonDb, &offsetsDbEntry{Ino: ino, Off: off})
	}
	contents, err := json.Marshal(jsonDb)
	offsetsMutex.Unlock()

	if err != nil {
		log.Fatalln("Could not marshal offsets to JSON: ", err.Error())
	}

	if _, err = fp.Write(contents); err != nil {
		log.Fatalln("Could not write contents to tmp file: ", err.Error())
	}

	if err = fp.Close(); err != nil {
		log.Fatalln("Could not close offsets file: ", err.Error())
	}

	if err = os.Rename(fp.Name(), config.GetOffsetsDb()); err != nil {
		log.Fatalln("Could not rename temp offsets db to permanent one")
	}
}

func offsetsSaver() {
	for {
		time.Sleep(time.Second)
		saveOffsets()
	}
}

// Return events that have not been processed back and mark server as offline temporarily
func (rg *receiverGroup) returnEvents(events []*lsd.RequestNewEventsEventT, receiverIndex int, evCh chan *lsd.RequestNewEventsEventT) {
	answerCh := make(chan bool, 1)
	rg.offlineChan <- &offlineEvent{idx: receiverIndex, offline: true, respCh: answerCh}
	<-answerCh // make sure that offline event was received

	initialLen := len(evCh)

	// separate goroutine is required so that write to outMessagesChan does not block and prevent server from returning online
	go func() {
		for _, ev := range events {
			rg.outChan <- ev
		}

		// get rid of all events that are in our buffer as well
		for i := 0; i < initialLen; i++ {
			rg.outChan <- (<-evCh)
		}
	}()

	time.Sleep(time.Second * 60)
	rg.offlineChan <- &offlineEvent{idx: receiverIndex, offline: false, respCh: answerCh}
	<-answerCh
}

func offsetsValid(events []*lsd.RequestNewEventsEventT, offsets []*lsd.ResponseOffsetsOffsetT) bool {
	requestedOffsets := make(map[uint64]map[uint64]uint64) // inode => (offset => true)

	for _, ev := range events {
		if requestedOffsets[*ev.Inode] == nil {
			requestedOffsets[*ev.Inode] = make(map[uint64]uint64)
		}
		requestedOffsets[*ev.Inode][*ev.Offset]++
	}

	success := true

	for _, resp := range offsets {
		if requestedOffsets[*resp.Inode] == nil {
			log.Errorf("Extra inode #%d received from server which was not present in response", *resp.Inode)
			success = false
			continue
		}

		if _, ok := requestedOffsets[*resp.Inode][*resp.Offset]; !ok {
			log.Errorf("Extra offset %d received for inode #%d in response", *resp.Offset, *resp.Inode)
			success = false
			continue
		}

		requestedOffsets[*resp.Inode][*resp.Offset]--
		if requestedOffsets[*resp.Inode][*resp.Offset] <= 0 {
			delete(requestedOffsets[*resp.Inode], *resp.Offset)
		}

		if len(requestedOffsets[*resp.Inode]) == 0 {
			delete(requestedOffsets, *resp.Inode)
		}
	}

	if len(requestedOffsets) > 0 {
		log.Errorf("Not all offsets were received from response")
		success = false
	}

	return success
}

func (rg *receiverGroup) messagesReceiver(rcv *receiver, receiverIndex int) {
	var msgid uint32
	var resp proto.Message
	var err error

	for {
		// Make an event batch so that we send big queries
		events := make([]*lsd.RequestNewEventsEventT, 0)
		events = append(events, <-rcv.events)
		l := len(rcv.events)

		var evBytes int64

		for i := 0; i < l; i++ {
			ev := <-rcv.events
			events = append(events, ev)

			evBytes += int64(ev.Size())
			if evBytes >= MAX_BYTES_PER_BATCH {
				break
			}
		}

		cl := gpbrpc.NewClient(rcv.addr, &lsd.Gpbrpc, &gpbrpc.GpbsCodec, CONN_TIMEOUT, REQUEST_TIMEOUT)
		ev := &lsd.RequestNewEvents{Events: events}
		msgid, resp, err = cl.Call(ev)
		cl.Close()

		if err != nil {
			log.Errorf("Got error during call: %s", err.Error())
			rg.returnEvents(events, receiverIndex, rcv.events)
			continue
		}

		if msgid != uint32(lsd.ResponseMsgid_RESPONSE_OFFSETS) {
			log.Errorf("Unexpected response from server: %+v", resp)
			rg.returnEvents(events, receiverIndex, rcv.events)
			continue
		}

		offsets := resp.(*lsd.ResponseOffsets).Offsets

		if !offsetsValid(events, offsets) {
			log.Errorf("Invalid offsets received from server %s, discarding", rcv.addr)
			rg.returnEvents(events, receiverIndex, rcv.events)
			continue
		}

		updateOutTrafficStats(events)

		for _, replyRow := range offsets {
			rg.replyChan <- replyRow
		}
	}
}

// send event to any of available receivers or return false if none available
// number of fragments is set to outEntry struct
func (rg *receiverGroup) tryToSendEvent(ev *lsd.RequestNewEventsEventT, oe *outEntry) bool {
	if len(ev.Lines) == 0 {
		return true
	}

	l := len(rg.receivers)

	if rg.prefixSharding {
		// we need to send lines based on content hash, so the event must be split by hash and sent appropriately
		// there can be up to len(rg.receivers) parts of the event
		onlineReceivers := make([]*receiver, 0, l)

		// check that there is enough room to fit split events into receivers
		for _, rcv := range rg.receivers {
			if rcv.offline {
				continue
			}

			if len(rcv.events) < cap(rcv.events) {
				onlineReceivers = append(onlineReceivers, rcv)
			}
		}

		if len(onlineReceivers) == 0 {
			return false
		}

		onlineCnt := len(onlineReceivers)
		hasher := fnv.New32a()

		splitEvents := make([]*lsd.RequestNewEventsEventT, onlineCnt)

		// When using prefixSharding, you specify line like "hash_idx:<something>"
		// The hash_idx itself is excluded from data that is sent to receivers if cutPrefix is set
		for _, ln := range ev.Lines {
			hashPos := strings.Index(ln, rg.prefixDelimiter)
			hasher.Reset()

			if hashPos > 0 {
				hasher.Write([]byte(ln[0:hashPos]))
				if rg.cutPrefix {
					ln = ln[hashPos+len(rg.prefixDelimiter):]
				}
			} else {
				// use the whole line as hash because no hash value was provided
				hasher.Write([]byte(ln))
			}

			idx := int(hasher.Sum32() % uint32(onlineCnt))

			if splitEvents[idx] == nil {
				sev := new(lsd.RequestNewEventsEventT)
				sev.Category = ev.Category
				sev.Inode = ev.Inode
				sev.Offset = ev.Offset
				sev.Lines = make([]string, 0)
				splitEvents[idx] = sev
			}

			splitEvents[idx].Lines = append(splitEvents[idx].Lines, ln)
		}

		// event can already be fragmented, so as we decrement pending fragments count
		// instead of setting it's value to 0
		oe.pendingFragments--

		for idx, sev := range splitEvents {
			if sev == nil {
				continue
			}

			// No busy check here because we already checked at the beginning.
			// Noone else writes to our queue so additional check is not required here
			onlineReceivers[idx].events <- sev
			oe.pendingFragments++
		}

		return true
	} else {
		// try to send immediately to any free worker
		for i := 0; i < l; i++ {
			rcv := rg.receivers[rg.curReceiverIdx]
			rg.curReceiverIdx = (rg.curReceiverIdx + 1) % l

			if rcv.offline {
				continue
			}

			select {
			case rcv.events <- ev:
				return true
			default:
			}
		}
	}

	return false
}

func processOfflineEv(receivers []*receiver, ev *offlineEvent) {
	receivers[ev.idx].offline = ev.offline
	if ev.offline {
		log.Infof("Server went offline: %s", receivers[ev.idx].addr)
	} else {
		log.Infof("Server went online: %s", receivers[ev.idx].addr)
	}
	ev.respCh <- true
}

func outgoingToString(outgoing []*outEntry) string {
	if outgoing == nil {
		return "(empty)"
	}

	parts := make([]string, 0)
	for _, row := range outgoing {
		parts = append(parts, fmt.Sprintf("{offset: %v, pendingFragments: %v}, ", row.offset, row.pendingFragments))
	}
	return strings.Join(parts, "")
}

func (rg *receiverGroup) eventsRouter() {
	outgoingMap := make(map[uint64][]*outEntry)      // inode => list of outgoing requests with done markers
	doneMap := make(map[uint64]map[uint64]*outEntry) // inode => map(offset => link to done marker)
	outChan := rg.outChan
	lastBusyTs := time.Now()

	var oe *outEntry
	var busyCnt int

	pendingChan := make(chan *lsd.RequestNewEventsEventT, 1) // channel for retries

	for {
		select {
		case ev := <-outChan:
			if doneMap[*ev.Inode] == nil || doneMap[*ev.Inode][*ev.Offset] == nil {
				oe = new(outEntry)
				oe.offset = *ev.Offset
				oe.pendingFragments = 1

				if _, ok := outgoingMap[*ev.Inode]; !ok {
					outgoingMap[*ev.Inode] = make([]*outEntry, 0)
				}

				outgoingMap[*ev.Inode] = append(outgoingMap[*ev.Inode], oe)
				if _, ok := doneMap[*ev.Inode]; !ok {
					doneMap[*ev.Inode] = make(map[uint64]*outEntry)
				}
				doneMap[*ev.Inode][*ev.Offset] = oe
			} else {
				oe = doneMap[*ev.Inode][*ev.Offset]
			}

			if rg.tryToSendEvent(ev, oe) {
				outChan = rg.outChan
				break
			} else {
				outChan = pendingChan
				go func() {
					receiversAddrs := make([]string, 0, len(rg.receivers))
					for _, rcv := range rg.receivers {
						receiversAddrs = append(receiversAddrs, rcv.addr)
					}

					now := time.Now()
					busyCnt++

					if now.Sub(lastBusyTs) > MAX_BUSY_REPORT_INTERVAL {
						log.Errorf("All receivers busy was %d times during last %d sec for %s", busyCnt,
							now.Sub(lastBusyTs)/time.Second, strings.Join(receiversAddrs, ", "))
						busyCnt = 0
						lastBusyTs = now
					}

					time.Sleep(time.Millisecond * 100)
					pendingChan <- ev
				}()
			}
		case offEv := <-rg.offlineChan:
			processOfflineEv(rg.receivers, offEv)
		case answer := <-rg.replyChan:
			ino := *answer.Inode
			off := *answer.Offset

			if doneMap[ino] == nil {
				log.Errorf("Could not find answer inode for inode %d", ino)
				break
			}

			if doneMap[ino][off] == nil {
				log.Errorf("Could not find offset result for offset=%d inode=%d", off, ino)
				break
			}

			if _, ok := outgoingMap[ino]; !ok {
				log.Errorf("Could not find outgoing list for inode %d, memory leaks and non-delivered events are possible", ino)
				break
			}

			doneMap[ino][off].pendingFragments--
			if doneMap[ino][off].pendingFragments == 0 {
				delete(doneMap[ino], off)
			}

			if len(doneMap[ino]) == 0 {
				delete(doneMap, ino)
			}

			// log.Debugf("Outgoing before compactification: %s", outgoingToString(outgoingMap[ino]))

			// compactify outgoing and update offsets if needed: we can update offsets to the last value of offset that has been successfully delivered
			minIdx := -1
			var lastDoneOff uint64

			outgoing := outgoingMap[ino]

			for i, oe := range outgoing {
				if oe.pendingFragments > 0 {
					break
				}
				minIdx = i
				lastDoneOff = oe.offset
			}

			if minIdx >= 0 {
				offsetsMutex.Lock()
				offsets[ino] = int64(lastDoneOff)
				offsetsMutex.Unlock()

				outgoingMap[ino] = outgoing[minIdx+1:]

				if len(outgoingMap[ino]) == 0 {
					delete(outgoingMap, ino)
				}
			}

			// log.Debugf("Outgoing after compactification: %+v, minIdx: %d", outgoingToString(outgoingMap[ino]), minIdx)
		}
	}
}

func eventsRouter() {
	receiverGroupsMutex.Lock()
	defer receiverGroupsMutex.Unlock()

	haveDefaultRoute := false

	for routingIdx, confRouting := range config.Routing {
		receivers := make([]*receiver, 0)

		for _, confRcv := range confRouting.Receivers {
			chSize := MAX_MESSAGES_PER_BATCH
			// buffered channels for prefix sharding must be bigger because each event is split to chunks for each receiver
			if confRouting.GetPrefixSharding() {
				if len(confRouting.Receivers) >= MAX_PREFIX_SHARDING_AMPLIFICATION {
					chSize *= MAX_PREFIX_SHARDING_AMPLIFICATION
				} else {
					chSize *= len(confRouting.Receivers)
				}
			}
			ch := make(chan *lsd.RequestNewEventsEventT, chSize)
			rcv := &receiver{events: ch, addr: *confRcv.Addr}
			receivers = append(receivers, rcv)
		}

		if len(receivers) == 0 {
			log.Fatalf("Bad receivers property for routing config in element #%d", routingIdx)
		}

		rg := new(receiverGroup)
		rg.receivers = receivers
		if confRouting.GetPrefixSharding() {
			rg.prefixSharding = true
			rg.prefixDelimiter = confRouting.GetPrefixDelimiter()
			rg.cutPrefix = confRouting.GetCutPrefix()
		}

		regexes := make([]*regexp.Regexp, 0)
		for _, cat := range confRouting.Categories {
			regStr := "^" + strings.Replace(regexp.QuoteMeta(cat), "\\*", ".*", -1) + "$"
			regexes = append(regexes, regexp.MustCompile(regStr))
		}

		rg.categories = regexes
		if len(regexes) == 0 {
			if haveDefaultRoute {
				log.Fatalf("Bad config: more than one default category config for routing config in element #%d", routingIdx)
			}

			rg.isDefaultRoute = true
			haveDefaultRoute = true
		}

		rg.outChan = make(chan *lsd.RequestNewEventsEventT, MAX_MESSAGES_PER_BATCH)
		rg.replyChan = make(chan *lsd.ResponseOffsetsOffsetT, 10)
		rg.offlineChan = make(chan *offlineEvent, 10)

		receiverGroups = append(receiverGroups, rg)
	}

	if !haveDefaultRoute {
		log.Fatalf("Bad config: no default route")
	}

	for _, rg := range receiverGroups {
		for rcvIdx, rcv := range rg.receivers {
			go rg.messagesReceiver(rcv, rcvIdx)
		}

		go rg.eventsRouter()
	}
}

// A goroutine to calculate event traffic speed
func calculateSpeed() {
	oldValues := make(map[string]int64)

	for {
		time.Sleep(TRAFFIC_SPEED_CALC_INTERVAL * time.Second)

		newValues := common.CloneStatMap(categoryOutTraffic, categoryOutTrafficMutex)
		speedMap := make(map[string]int64)

		for category, traffic := range newValues {
			speedMap[category] = (traffic - oldValues[category]) / TRAFFIC_SPEED_CALC_INTERVAL
		}

		categoryOutTrafficSpeedMutex.Lock()
		categoryOutTrafficSpeed = speedMap
		categoryOutTrafficSpeedMutex.Unlock()

		oldValues = newValues
	}
}

func updateOutTrafficStats(events []*lsd.RequestNewEventsEventT) {
	categoryOutTrafficMutex.Lock()
	defer categoryOutTrafficMutex.Unlock()

	for _, ev := range events {
		for _, ln := range ev.Lines {
			categoryOutTraffic[*ev.Category] += int64(len(ln))
		}
	}
}

func init() {
	expvar.Publish("out_traffic", expvar.Func(func() interface{} { return common.CloneStatMap(categoryOutTraffic, categoryOutTrafficMutex) }))
	expvar.Publish("out_traffic_speed", expvar.Func(func() interface{} { return common.CloneStatMap(categoryOutTrafficSpeed, categoryOutTrafficSpeedMutex) }))
}

func Run(clientConfig *lsd.LsdConfigClientConfigT) {
	if clientConfig == nil {
		return
	}

	config = clientConfig

	time.Sleep(time.Millisecond * 500) // let server start listening local port if server and client are the same

	readOffsets()

	log.Println("Saving offsets db")
	saveOffsets()

	eventsRouter()

	go offsetsSaver()
	go processUsageRequests()

	go runWatcher(config.GetSourceDir(), fsNotifyChan)
	go fsnotifyRouter()
	go calculateSpeed()

	go common.IoManagerThread(MAX_IO_THREADS, ioRequestChan, ioFreeChan)
	go common.IoManagerThread(MAX_DELETE_THREADS, deleteIoRequestChan, deleteIoFreeChan)
}
