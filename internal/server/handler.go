package server

import (
	"badoo/_packages/gpbrpc"
	"github.com/badoo/lsd/internal/traffic"
	"github.com/badoo/lsd/proto"
	"context"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"
)

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
	defaultCategory := categorySettings{
		maxFileSize:        config.GetMaxFileSize(),
		fileRotateInterval: config.GetFileRotateInterval(),
	}
	allCategories := make([]categorySettings, 0)
	for _, row := range config.GetPerCategorySettings() {
		settings := defaultCategory
		if row.GetMaxFileSize() != 0 {
			settings.maxFileSize = row.GetMaxFileSize()
		}
		if row.GetFileRotateInterval() != 0 {
			settings.fileRotateInterval = row.GetFileRotateInterval()
		}
		// we can have only per category compression setting
		// no default ones
		settings.gzip = row.GetGzip()
		settings.gzipParallel = row.GetGzipParallel()

		if settings.gzipParallel > 1 && settings.maxFileSize < 1<<20 {
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
		settings.patterns = patterns
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
		listeners:          make(map[string]*listener),
		baseDir:            strings.TrimRight(config.GetTargetDir(), "/"),
		categories:         allCategories,
		defaultCategory:    defaultCategory,
		errorSleepInterval: time.Second * time.Duration(config.GetErrorSleepInterval()),
	}, nil
}

type handler struct {
	ctx            context.Context
	cancel         context.CancelFunc
	listeners      map[string]*listener
	trafficManager *traffic.Manager

	sync.WaitGroup
	sync.RWMutex

	// settings related stuff
	baseDir            string
	categories         []categorySettings
	defaultCategory    categorySettings
	errorSleepInterval time.Duration
}

func (h *handler) getSettingsForCategory(name string) categorySettings {
	for _, category := range h.categories {
		for _, pattern := range category.patterns {
			if pattern.MatchString(name) {
				return category
			}
		}
	}
	return h.defaultCategory
}

func (h *handler) getListenerForCategory(category string) *listener {

	// most requests will end up here, because we create goroutine only 1 time per category
	h.RLock()
	l, ok := h.listeners[category]
	if ok {
		h.RUnlock()
		return l
	}
	h.RUnlock()
	// recheck listener after lock is ours
	// and then create it
	h.Lock()
	defer h.Unlock()
	l, ok = h.listeners[category]
	if ok {
		return l
	}
	l = &listener{
		ctx:            h.ctx,
		categoryName:   category,
		baseDir:        h.baseDir,
		categoryPath:   h.baseDir + "/" + category,
		trafficManager: h.trafficManager,
		settings:       h.getSettingsForCategory(category),
		inCh:           make(chan *categoryEvent, CATEGORY_EVENT_BUFFER_SIZE),
	}
	h.Add(1)
	go func() {
		l.mainLoop(h.errorSleepInterval)
		h.Done()
	}()
	h.listeners[category] = l
	return l
}

func (h *handler) RequestNewEvents(rctx gpbrpc.RequestT, request *lsd.RequestNewEvents) gpbrpc.ResultT {

	if h.ctx.Err() != nil {
		return lsd.Gpbrpc.ErrorGeneric("Server is shutting down")
	}

	answerCh := make(chan *lsd.ResponseOffsetsOffsetT, len(request.Events))

	for _, ev := range request.Events {
		l := h.getListenerForCategory(ev.GetCategory())
		l.inCh <- &categoryEvent{ev, answerCh}
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

func (h *handler) Shutdown() {
	h.cancel()
	h.Wait()
}
