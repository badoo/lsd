package server

import (
	"badoo/_packages/log"
	"github.com/badoo/lsd/internal/traffic"
	"github.com/badoo/lsd/proto"
	"context"
	"fmt"
	"io"
	"os"
	"time"
)

const CATEGORY_EVENT_BUFFER_SIZE = 100
const CATEGORY_READ_DIR_BUFFER_SIZE = 1000
const FORCE_FLUSH_INTERVAL = time.Millisecond * 50

type categoryEvent struct {
	*lsd.RequestNewEventsEventT
	answerCh chan *lsd.ResponseOffsetsOffsetT
}

type listener struct {
	ctx            context.Context
	baseDir        string
	categoryName   string
	categoryPath   string
	settings       categorySettings
	trafficManager *traffic.Manager
	inCh           chan *categoryEvent
}

func (l *listener) mainLoop(errorSleepInterval time.Duration) {
	for {
		err := l.processEvents() // we should never exit from here if everything is ok
		if err == nil {
			// asked to exit
			return
		}
		log.Errorf("%s category loop failed with error: %v", l.categoryName, err)
		wakeUpCh := time.After(errorSleepInterval)
		// discard all events while sleeping
	outerLoop:
		for {
			select {
			case <-wakeUpCh:
				break outerLoop
			case ev := <-l.inCh:
				ev.answerCh <- nil
			case <-l.ctx.Done():
				// asked to exit
				return
			}
		}
	}
}

func (l *listener) processEvents() error {

	err := l.createCategoryDir()
	if err != nil {
		return fmt.Errorf("failed to create category dir: %v", err)
	}

	writer, err := l.prepareWriter()
	if err != nil {
		return fmt.Errorf("failed to reset category writer: %v", err)
	}
	// close files and discard all queued, but not written events
	defer writer.close()

	rotateTimeOut := time.Second * time.Duration(l.settings.fileRotateInterval)
	rotateTimeoutCh := time.After(rotateTimeOut)
	flushTicker := time.NewTicker(FORCE_FLUSH_INTERVAL)
	defer flushTicker.Stop()
	for {
		select {
		case event := <-l.inCh:
			rotated, err := writer.write(event)
			if err != nil {
				return fmt.Errorf("failed to write event: %v", err)
			}
			if rotated {
				rotateTimeoutCh = time.After(rotateTimeOut)
			}

		case <-rotateTimeoutCh:
			rotateTimeoutCh = time.After(rotateTimeOut)
			if writer.isEmpty() {
				// nothing has been written after last rotate
				break
			}
			err = writer.rotate()
			if err != nil {
				return fmt.Errorf("failed to rotate on time threshold: %v", err)
			}
		case <-flushTicker.C:
			err := writer.flush()
			if err != nil {
				return fmt.Errorf("failed to flush buffer (periodic): %v", err)
			}
		case <-l.ctx.Done():
			// asked to exit
			return nil
		}
	}
}

func (l *listener) createCategoryDir() error {
	err := os.Mkdir(l.categoryPath, 0777)
	if err != nil && !os.IsExist(err) {
		return fmt.Errorf("failed to create %s: %v", l.categoryPath, err)
	}
	return nil
}

func (l *listener) prepareWriter() (*categoryWriter, error) {
	lastChunk, err := l.findLastChunk()
	if err != nil {
		return nil, fmt.Errorf("failed to find last chunkInfo: %v", err)
	}
	w, err := newCategoryWriter(l.baseDir, l.settings, l.categoryPath, lastChunk, l.trafficManager)
	if err != nil {
		return nil, fmt.Errorf("faield to reset writer: %v", err)
	}
	if w.size == 0 {
		return w, nil
	}
	err = w.rotate()
	if err != nil {
		return w, fmt.Errorf("initial rotate failed: %v", err)
	}
	return w, nil
}

func (l *listener) findLastChunk() (*chunkInfo, error) {

	result := &chunkInfo{
		category: l.categoryName,
		date:     time.Now().Format(DATE_FORMAT),
		counter:  1,
	}
	fp, err := os.Open(l.categoryPath)
	if err != nil {
		return result, fmt.Errorf("failed to getWriter dir %s: %v", l.categoryPath, err)
	}
	defer func() {
		err = fp.Close()
		if err != nil {
			log.Errorf("failed to close dir %s: %v", l.categoryPath, err)
		}
	}()

	for {
		fis, err := fp.Readdir(CATEGORY_READ_DIR_BUFFER_SIZE)
		if err == io.EOF {
			break
		}
		if err != nil {
			return result, fmt.Errorf("failed to read dir %s: %v", l.categoryPath, err)
		}
		for _, fi := range fis {
			if fi.Name() == "." || fi.Name() == ".." {
				continue
			}
			if !fi.Mode().IsRegular() {
				continue
			}
			date, counter, err := parseFileName(l.categoryName, fi.Name())
			if err != nil {
				continue
			}
			newChunk := &chunkInfo{
				category: l.categoryName,
				date:     date,
				counter:  counter,
			}
			if newChunk.isNewerThan(result) {
				result = newChunk
			}
		}
	}
	return result, nil
}
