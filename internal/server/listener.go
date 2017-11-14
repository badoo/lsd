package server

import (
	"badoo/_packages/log"
	"github.com/badoo/lsd/internal/traffic"
	"github.com/badoo/lsd/proto"
	"bufio"
	"bytes"
	"compress/gzip"
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
			err := l.maybeDecompressEvent(event.RequestNewEventsEventT)
			if err != nil {
				// this event has not been accepted, so we will not discard it in
				// writer.close
				event.answerCh <- nil
				return fmt.Errorf("failed to decompress event: %v", err)
			}
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

func (w *listener) maybeDecompressEvent(ev *lsd.RequestNewEventsEventT) error {

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
