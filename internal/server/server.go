package server

import (
	"badoo/_packages/gpbrpc"
	"badoo/_packages/log"
	"github.com/badoo/lsd/internal/common"
	"bufio"
	"errors"
	"expvar"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/badoo/lsd/proto"
)

type (
	GpbrpcType struct{}

	GpbContext struct{}

	WriteRequest struct {
		req      *lsd.RequestNewEventsEventT
		answerCh chan *lsd.ResponseOffsetsOffsetT
	}

	Setting struct {
		categories         []*regexp.Regexp
		maxFileSize        uint64
		fileRotateInterval uint64
		chunkOutput        bool
	}
)

const (
	CATEGORY_EVENT_BUFFER_SIZE = 50
	WRITE_BUFFER_SIZE          = 1 << 16 // how many bytes to use as a filesystem write buffer

	MAX_LINES_PER_BATCH = 1000
	DATE_FORMAT         = "2006-01-02"

	TRAFFIC_SPEED_CALC_INTERVAL = 10 // seconds

	MAX_IO_THREADS = 16 // how many OS threads can be spawned for IO operations. TODO: make it configurable
)

var (
	ioRequestChan = make(chan chan bool) // channel for getting "ticket" for new concurrent io operation
	ioFreeChan    = make(chan bool, 10)  // channel for freeing io operation

	config *lsd.LsdConfigServerConfigT

	// per-category traffic stats
	categoryInTrafficMutex = new(sync.Mutex)
	categoryInTraffic      = make(map[string]int64)

	// per-category traffic stats (average speed)
	categoryInTrafficSpeedMutex = new(sync.Mutex)
	categoryInTrafficSpeed      = make(map[string]int64)

	// per-category goroutine
	categoryEventsMutex = new(sync.Mutex)
	categoryEvents      = make(map[string]chan WriteRequest)

	// per-category settings
	perCategorySettings = make([]Setting, 0)

	// errors
	errNotCategoryFile    = errors.New("Filename does not start with category name")
	errNoCounter          = errors.New("No counter in filename")
	errTooManyUnderscores = errors.New("Too many underscores in filename")
)

func makeFileName(dir, category string, lastDate *string, counter *uint64) string {
	curDate := time.Now().Format(DATE_FORMAT)
	if curDate != *lastDate {
		*lastDate = curDate
		*counter = 1
	} else {
		(*counter)++
	}

	return fmt.Sprintf("%s/%s-%s_%06d", dir, category, curDate, *counter)
}

// parse filename like "<category>-<date>-<counter>"
func parseFileName(filename, category string) (string, uint64, error) {
	prefix := category + "-"

	if !strings.HasPrefix(filename, prefix) {
		return "", 0, errNotCategoryFile
	}

	filename = strings.TrimPrefix(filename, prefix)
	parts := strings.Split(filename, "_")
	if len(parts) == 1 {
		return "", 0, errNoCounter
	}

	if len(parts) > 2 {
		return "", 0, errTooManyUnderscores
	}

	dt := parts[0]

	cnt, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		return "", 0, err
	}

	return dt, cnt, nil
}

func createCurrentSymlink(currentLinkName string, linkName string) error {
	err := os.Remove(currentLinkName)
	if err != nil && !os.IsNotExist(err) {
		return err
	}

	err = os.Symlink(filepath.Base(linkName), currentLinkName)
	if err != nil {
		return err
	}

	return nil
}

func getCategorySettings(category string) (maxSz uint64, rotInterv uint64, chunkOutput bool) {
	maxSz = config.GetMaxFileSize()
	rotInterv = config.GetFileRotateInterval()
	chunkOutput = config.GetChunkOutput()

	for _, setting := range perCategorySettings {
		for _, catRegex := range setting.categories {
			if catRegex.MatchString(category) {
				maxSz = setting.maxFileSize
				rotInterv = setting.fileRotateInterval
				chunkOutput = setting.chunkOutput
				return
			}
		}
	}

	return
}

// A goroutine for processing write events for specified category
func processCategoryEvents(category string, ch chan WriteRequest) {
	defer func() {
		categoryEventsMutex.Lock()
		ch := categoryEvents[category]
		delete(categoryEvents, category)
		categoryEventsMutex.Unlock()

		// TODO: make better solution to this problem
		// ugly hack to drain queue: wait a second so that all cached channels are received
		// and send nil to all answerCh
		time.Sleep(time.Second)

		for {
			select {
			case ev := <-ch:
				ev.answerCh <- nil
			default:
				return
			}
		}
	}()

	maxSz, rotInterv, chunkOutput := getCategorySettings(category)

	if chunkOutput {
		processCategoryEventsChunked(category, ch, maxSz, rotInterv)
	} else {
		processCategoryEventsSimple(category, ch)
	}
}

func writeEventsToFp(fp *os.File, category string, ch chan WriteRequest, firstEv WriteRequest) (success bool, bytesWritten int64) {
	currentFp := bufio.NewWriterSize(fp, WRITE_BUFFER_SIZE)

	messages := make([]WriteRequest, 0, len(ch))
	messages = append(messages, firstEv)
	totalLines := 0

	for l := len(ch); l > 0; l-- {
		req := <-ch
		messages = append(messages, req)

		if totalLines += len(req.req.Lines); totalLines >= MAX_LINES_PER_BATCH {
			break
		}
	}

	success = func() bool {
		var err error

		common.RequestIO(ioRequestChan)
		defer common.ReleaseIO(ioFreeChan)

		if err = syscall.Flock(int(fp.Fd()), syscall.LOCK_EX); err != nil {
			log.Errorf("Could not lock file: %s", err.Error())
			return false
		}
		defer syscall.Flock(int(fp.Fd()), syscall.LOCK_UN)

		for _, req := range messages {
			for _, ln := range req.req.Lines {
				if ln[len(ln)-1] != '\n' {
					log.Errorf("Incomplete line: '%s'", ln)
				} else if strings.Count(ln, "\n") > 1 {
					log.Errorf("More than a single \\n in line: '%s'", strings.Replace(ln, "\n", "\\n", -1))
				}

				if _, err = currentFp.WriteString(ln); err != nil {
					log.Errorf("Could not write to target file: %s", err.Error())
					return false
				}
				bytesWritten += int64(len(ln))
			}
		}

		if err = currentFp.Flush(); err != nil {
			log.Errorf("Could not flush data to target file: %s", err.Error())
			return false
		}

		return true
	}()

	if !success {
		for _, req := range messages {
			req.answerCh <- nil
		}
		return
	}

	categoryInTrafficMutex.Lock()
	categoryInTraffic[category] += bytesWritten
	categoryInTrafficMutex.Unlock()

	for _, req := range messages {
		req.answerCh <- &lsd.ResponseOffsetsOffsetT{Inode: req.req.Inode, Offset: req.req.Offset}
	}

	return
}

func processCategoryEventsChunked(category string, ch chan WriteRequest, maxSz uint64, rotInterv uint64) {
	path := config.GetTargetDir() + "/" + category
	err := os.Mkdir(path, 0777)
	if err != nil && !os.IsExist(err) {
		log.Errorf("Could not mkdir(%s): %s", path, err.Error())
		return
	}

	var lastDate string
	var counter uint64
	var osFlags = os.O_APPEND | os.O_CREATE | os.O_WRONLY
	var currentFileFp *os.File

	currentLinkName := path + "/" + category + "_current"
	currentFileName := makeFileName(path, category, &lastDate, &counter)

	fp, err := os.Open(path)
	if err != nil {
		log.Errorf("Could not open %s: %s", path, err.Error())
		return
	}
	defer fp.Close()

	for {
		fis, err := fp.Readdir(100)

		if err != nil {
			if err == io.EOF {
				break
			} else {
				log.Errorf("Could not readdir: %s", err.Error())
				return
			}
		}

		for _, fi := range fis {
			if !fi.Mode().IsRegular() {
				continue
			}

			dt, cnt, err := parseFileName(fi.Name(), category)
			if err != nil {
				log.Debugf("Could not parse filename %s: %s", fi.Name(), err)
				continue
			}

			if dt != lastDate {
				continue
			}

			if cnt >= counter {
				counter = cnt
				currentFileName = makeFileName(path, category, &lastDate, &counter)
			}
		}
	}

	var currentBytesWritten int64
	func() {
		common.RequestIO(ioRequestChan)
		defer common.ReleaseIO(ioFreeChan)

		currentFileFp, err = os.OpenFile(currentFileName, osFlags, 0666)
	}()

	if err != nil {
		log.Errorf("Could not create target file: %s", err.Error())
		return
	}

	createCurrentSymlink(currentLinkName, currentFileFp.Name())

	lastChangeTs := time.Now().Unix()
	forceRecreateCh := time.After(time.Second * time.Duration(rotInterv + 1))

	for {
		var bytesWritten int64

		select {
		case ev := <-ch:
			var success bool
			success, bytesWritten = writeEventsToFp(currentFileFp, category, ch, ev)
			if !success {
				return
			}
		case <-forceRecreateCh:
			forceRecreateCh = nil
		}

		currentBytesWritten += bytesWritten

		curTs := time.Now().Unix()
		if currentBytesWritten >= int64(maxSz) || (currentBytesWritten > 0 && curTs > lastChangeTs+int64(rotInterv)) {
			if err = currentFileFp.Close(); err != nil {
				log.Errorf("Could not close current file pointer: %s", err.Error())
				return
			}

			currentFileName = makeFileName(path, category, &lastDate, &counter)

			func() {
				common.RequestIO(ioRequestChan)
				defer common.ReleaseIO(ioFreeChan)

				currentFileFp, err = os.OpenFile(currentFileName, osFlags, 0666)
			}()

			if err != nil {
				log.Errorf("Could not create target file: %s", err.Error())
				return
			}

			err = createCurrentSymlink(currentLinkName, currentFileFp.Name())
			if err != nil {
				log.Errorf("Could not create current symlink: %s", err.Error())
				return
			}

			lastChangeTs = curTs
			currentBytesWritten = 0
			forceRecreateCh = time.After(time.Second * time.Duration(rotInterv + 1))
		}
	}
}

// goroutine for writing events to plain file "category.log"
func processCategoryEventsSimple(category string, ch chan WriteRequest) {
	path := config.GetTargetDir() + "/" + category + ".log"

	var fp *os.File
	var err error

	var reopenCh <-chan time.Time

	for {
		select {
		case <-reopenCh:
			if fp == nil {
				log.Errorf("Internal inconsistency: reopen was triggered when file is not open for category %s", category)
			} else {
				if err = fp.Close(); err != nil {
					log.Errorf("Could not close file: %s", err.Error())
					return
				}

				fp = nil
			}
		case ev := <-ch:
			if fp == nil {
				fp, err = os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
				if err != nil {
					log.Errorf("Could not open category file: %s", err.Error())
					return
				}

				reopenCh = time.After(time.Second)
			}

			if success, _ := writeEventsToFp(fp, category, ch, ev); !success {
				return
			}
		}
	}
}

func (ctx *GpbContext) RequestNewEvents(rctx gpbrpc.RequestT, request *lsd.RequestNewEvents) gpbrpc.ResultT {
	if config == nil {
		return gpbrpc.Result(&lsd.ResponseGeneric{ErrorCode: proto.Int32(int32(lsd.Errno_ERRNO_GENERIC)), ErrorText: proto.String("No server config defined")})
	}

	log.Debugln("Received new request with new events")

	answerCh := make(chan *lsd.ResponseOffsetsOffsetT, len(request.Events))

	for _, ev := range request.Events {
		categoryEventsMutex.Lock()
		ch, ok := categoryEvents[*ev.Category]
		if !ok {
			ch = make(chan WriteRequest, CATEGORY_EVENT_BUFFER_SIZE)
			go processCategoryEvents(*ev.Category, ch)
			categoryEvents[*ev.Category] = ch
		}
		categoryEventsMutex.Unlock()

		ch <- WriteRequest{req: ev, answerCh: answerCh}
	}

	success := true

	resp := make([]*lsd.ResponseOffsetsOffsetT, len(request.Events))
	for i := 0; i < len(request.Events); i++ {
		resp[i] = <-answerCh
		if resp[i] == nil {
			success = false
		}
	}

	if !success {
		return gpbrpc.Result(&lsd.ResponseGeneric{ErrorCode: proto.Int32(int32(lsd.Errno_ERRNO_GENERIC)), ErrorText: proto.String("Failed to write results")})
	}

	return gpbrpc.Result(&lsd.ResponseOffsets{Offsets: resp})
}

// A goroutine to calculate event traffic speed
func calculateSpeed() {
	oldValues := make(map[string]int64)

	for {
		time.Sleep(TRAFFIC_SPEED_CALC_INTERVAL * time.Second)

		newValues := common.CloneStatMap(categoryInTraffic, categoryInTrafficMutex)
		speedMap := make(map[string]int64)

		for category, traffic := range newValues {
			speedMap[category] = (traffic - oldValues[category]) / TRAFFIC_SPEED_CALC_INTERVAL
		}

		categoryInTrafficSpeedMutex.Lock()
		categoryInTrafficSpeed = speedMap
		categoryInTrafficSpeedMutex.Unlock()

		oldValues = newValues
	}
}

func init() {
	expvar.Publish("in_traffic", expvar.Func(func() interface{} { return common.CloneStatMap(categoryInTraffic, categoryInTrafficMutex) }))
	expvar.Publish("in_traffic_speed", expvar.Func(func() interface{} { return common.CloneStatMap(categoryInTrafficSpeed, categoryInTrafficSpeedMutex) }))
}

func Setup(serverConfig *lsd.LsdConfigServerConfigT) {
	config = serverConfig

	if serverConfig.GetPerCategorySettings() != nil {
		for _, row := range serverConfig.GetPerCategorySettings() {
			cats := make([]*regexp.Regexp, 0)
			for _, str := range row.GetCategories() {
				regStr := "^" + strings.Replace(regexp.QuoteMeta(str), "\\*", ".*", -1) + "$"
				cats = append(cats, regexp.MustCompile(regStr))
			}

			maxSz := row.GetMaxFileSize()
			if maxSz == 0 {
				maxSz = serverConfig.GetMaxFileSize()
			}

			rotInterv := row.GetFileRotateInterval()
			if rotInterv == 0 {
				rotInterv = serverConfig.GetFileRotateInterval()
			}

			chunkOutput := serverConfig.GetChunkOutput()
			if row.ChunkOutput != nil {
				chunkOutput = row.GetChunkOutput()
			}

			perCategorySettings = append(perCategorySettings, Setting{
				categories:         cats,
				maxFileSize:        maxSz,
				fileRotateInterval: rotInterv,
				chunkOutput:        chunkOutput,
			})
		}
	}

	go calculateSpeed()
	go common.IoManagerThread(MAX_IO_THREADS, ioRequestChan, ioFreeChan)
}
