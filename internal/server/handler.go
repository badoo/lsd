package server

import (
	"badoo/_packages/gpbrpc"
	"badoo/_packages/log"
	"github.com/badoo/lsd/internal/server/category"
	"github.com/badoo/lsd/internal/traffic"
	"github.com/badoo/lsd/proto"
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"regexp"
	"strings"
	"sync"
	"time"
)

const CATEGORY_EVENT_BUFFER_SIZE = 100
const FORCE_FLUSH_INTERVAL = time.Millisecond * 50

type GPBHandler interface {
	RequestNewEvents(rctx gpbrpc.RequestT, request *lsd.RequestNewEvents) gpbrpc.ResultT
	Shutdown()
}

func NewHandler(config *lsd.LsdConfigServerConfigT, trafficManager *traffic.Manager) (GPBHandler, error) {

	if config == nil {
		return &emptyHandler{}, nil
	}

	if config.GetMaxFileSize() == 0 {
		return nil, fmt.Errorf("default max file size cannot be 0")
	}
	if config.GetFileRotateInterval() == 0 {
		return nil, fmt.Errorf("default file rotate interval cannot be 0")
	}
	defaultCategory := category.Settings{
		Mode:               config.GetMode(),
		MaxFileSize:        config.GetMaxFileSize(),
		FileRotateInterval: config.GetFileRotateInterval(),
	}
	allCategories := make([]category.Settings, 0)
	for _, row := range config.GetPerCategorySettings() {
		settings := defaultCategory
		if row.GetMode() != settings.Mode {
			settings.Mode = row.GetMode()
		}
		if row.GetMaxFileSize() != 0 {
			settings.MaxFileSize = row.GetMaxFileSize()
		}
		if row.GetFileRotateInterval() != 0 {
			settings.FileRotateInterval = row.GetFileRotateInterval()
		}
		// we can have only per category compression setting
		// no default ones
		settings.Gzip = row.GetGzip()
		settings.GzipParallel = row.GetGzipParallel()

		if settings.GzipParallel > 1 && settings.MaxFileSize < 1<<20 {
			// no profit to use parallel gzip on such a small chunk
			return nil, fmt.Errorf("parallel gzip is only allowed for 1mb+ chunks: %v", row)
		}

		patterns := make([]*regexp.Regexp, 0)
		for _, str := range row.GetCategories() {
			regex, err := regexp.Compile("^" + strings.Replace(regexp.QuoteMeta(str), "\\*", ".*", -1) + "$")
			if err != nil {
				return nil, fmt.Errorf("failed to compile regex for %v: %v", str, err)
			}
			patterns = append(patterns, regex)
		}
		settings.Patterns = patterns
		allCategories = append(allCategories, settings)
	}

	trafficManager.Publish()
	go func() {
		ticker := time.Tick(time.Duration(config.GetTrafficStatsRecalcInterval()) * time.Second)
		for {
			<-ticker
			trafficManager.Recalculate(config.GetTrafficStatsRecalcInterval())
		}
	}()

	ctx, cf := context.WithCancel(context.Background())
	return &handler{
		ctx:                ctx,
		cancel:             cf,
		trafficManager:     trafficManager,
		categoryChannels:   make(map[string]chan *category.Event),
		baseDir:            strings.TrimRight(config.GetTargetDir(), "/"),
		categories:         allCategories,
		defaultCategory:    defaultCategory,
		errorSleepInterval: time.Second * time.Duration(config.GetErrorSleepInterval()),
	}, nil
}

type handler struct {
	ctx              context.Context
	cancel           context.CancelFunc
	categoryChannels map[string]chan *category.Event
	trafficManager   *traffic.Manager

	sync.WaitGroup
	sync.RWMutex

	// settings related stuff
	baseDir            string
	categories         []category.Settings
	defaultCategory    category.Settings
	errorSleepInterval time.Duration
}

func (h *handler) RequestNewEvents(rctx gpbrpc.RequestT, request *lsd.RequestNewEvents) gpbrpc.ResultT {

	if h.ctx.Err() != nil {
		return lsd.Gpbrpc.ErrorGeneric("Server is shutting down")
	}

	answerCh := make(chan *lsd.ResponseOffsetsOffsetT, len(request.Events))

	for _, ev := range request.Events {
		err := h.maybeDecompressEvent(ev)
		if err != nil {
			log.Errorf("failed to decompress event: %v", err)
			// we cannot accept this event, so discard it immediately
			answerCh <- nil
			continue
		}
		h.getListenCh(ev.GetCategory()) <- &category.Event{RequestNewEventsEventT: ev, AnswerCh: answerCh}
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
		return lsd.Gpbrpc.ErrorGeneric("Failed to write results")
	}
	return gpbrpc.Result(&lsd.ResponseOffsets{Offsets: resp})
}

func (h *handler) getSettingsForCategory(name string) category.Settings {
	for _, cat := range h.categories {
		for _, pattern := range cat.Patterns {
			if pattern.MatchString(name) {
				return cat
			}
		}
	}
	return h.defaultCategory
}

func (h *handler) getListenCh(cat string) chan *category.Event {

	// most requests will end up here, because we create goroutine only 1 time per cat
	h.RLock()
	ch, ok := h.categoryChannels[cat]
	if ok {
		h.RUnlock()
		return ch
	}
	h.RUnlock()
	// recheck listener after lock is ours
	// and then create it
	h.Lock()
	defer h.Unlock()
	ch, ok = h.categoryChannels[cat]
	if ok {
		return ch
	}
	inCh := make(chan *category.Event, CATEGORY_EVENT_BUFFER_SIZE)
	settings := h.getSettingsForCategory(cat)
	h.Add(1)
	go func() {
		h.listenLoop(cat, settings, inCh)
		h.Done()
	}()
	h.categoryChannels[cat] = inCh
	return inCh
}

func (h *handler) maybeDecompressEvent(ev *lsd.RequestNewEventsEventT) error {

	if !ev.GetIsCompressed() {
		return nil
	}

	lines := ev.GetLines()
	if len(lines) == 0 {
		return nil
	}
	// TODO(antoxa): replace Buffer with a custom string reader, to avoid copying
	// TODO(antoxa): reuse gzip reader instead of allocating
	r := bytes.NewBufferString(lines[0])
	gz, err := gzip.NewReader(r)
	if err != nil {
		return fmt.Errorf("bad gzip header for '%s': %v", ev.GetCategory(), err)
	}

	rawLines, err := func() ([]string, error) {
		// TODO(antoxa): reuse buffered reader instead of allocating

		// FIXME(antoxa): expect to read no more than fs write buffer size at once
		// this assumption is probably incorrect and we should estimate based on MAX_LINES_PER_BATCH
		// or just send uncompressed data length from the client
		br := bufio.NewReader(gz)

		var result []string
		for {
			line, err := br.ReadString('\n')

			if err == io.EOF {
				break
			}
			if err != nil {
				return nil, err
			}

			result = append(result, line)
		}
		return result, nil
	}()

	if err != nil {
		return fmt.Errorf("bad gzip data for '%s': %v", ev.GetCategory(), err)
	}

	ev.Lines = rawLines
	*ev.IsCompressed = false // avoid 1 alloc and importing proto

	return nil
}

func (h *handler) listenLoop(categoryName string, settings category.Settings, inCh chan *category.Event) {
	for {
		err := h.processEvents(categoryName, settings, inCh) // we should never exit from here if everything is ok
		if err == nil {
			// asked to exit
			return
		}
		log.Errorf("%s category loop failed with error: %v", categoryName, err)
		wakeUpCh := time.After(h.errorSleepInterval)
		// discard all events while sleeping
	outerLoop:
		for {
			select {
			case <-wakeUpCh:
				break outerLoop
			case ev := <-inCh:
				ev.AnswerCh <- nil
			case <-h.ctx.Done():
				// asked to exit
				return
			}
		}
	}
}

func (h *handler) processEvents(categoryName string, settings category.Settings, inCh chan *category.Event) error {

	var writer category.Writer
	categoryPath := h.baseDir + "/" + categoryName
	if settings.Mode == lsd.LsdConfigServerConfigT_PLAIN_LOG {
		writer = category.NewPlainWriter(categoryName, categoryPath, settings, h.trafficManager)
	} else {
		writer = category.NewChunkedWriter(h.baseDir, categoryName, categoryPath, settings, h.trafficManager)
	}
	err := writer.Prepare()
	if err != nil {
		return fmt.Errorf("failed to prepare category writer: %v", err)
	}
	// close files and discard all queued, but not written events
	defer func() {
		closeErr := writer.Close()
		if closeErr != nil {
			log.Errorf("failed to close writer: %v", closeErr)
		}
	}()

	rotateTimeOut := time.Second * time.Duration(settings.FileRotateInterval)
	rotateTimeoutCh := time.After(rotateTimeOut)
	flushTicker := time.NewTicker(FORCE_FLUSH_INTERVAL)
	defer flushTicker.Stop()
	for {
		select {
		case event := <-inCh:
			rotated, err := writer.Write(event)
			if err != nil {
				return fmt.Errorf("failed to write event: %v", err)
			}
			if rotated {
				rotateTimeoutCh = time.After(rotateTimeOut)
			}

		case <-rotateTimeoutCh:
			rotateTimeoutCh = time.After(rotateTimeOut)
			err = writer.Rotate()
			if err != nil {
				return fmt.Errorf("failed to rotate on time threshold: %v", err)
			}
		case <-flushTicker.C:
			err := writer.Flush()
			if err != nil {
				return fmt.Errorf("failed to flush buffer (periodic): %v", err)
			}
		case <-h.ctx.Done():
			// asked to exit
			return nil
		}
	}
}

func (h *handler) Shutdown() {
	h.cancel()
	h.Wait()
}
