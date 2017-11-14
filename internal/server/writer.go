package server

import (
	"badoo/_packages/log"
	"github.com/badoo/lsd/internal/traffic"
	"github.com/badoo/lsd/proto"
	"bufio"
	"fmt"
	"os"
)

const EVENTS_BUFFER_SIZE = 1000
const WRITE_BUFFER_SIZE = 1 << 20 // 1mb

func newCategoryWriter(baseDir string, settings categorySettings, categoryPath string, chunk *chunkInfo, trafficManager *traffic.Manager) (*categoryWriter, error) {

	w := &categoryWriter{

		writeBuffer:  bufio.NewWriterSize(nil, WRITE_BUFFER_SIZE),
		eventsBuffer: make([]*categoryEvent, 0, EVENTS_BUFFER_SIZE),
		settings:     settings,

		baseDir:        baseDir,
		categoryPath:   categoryPath,
		chunk:          chunk,
		trafficManager: trafficManager,
	}
	err := w.reset()
	if err != nil {
		return w, fmt.Errorf("failed to initialize: %v", err)
	}
	return w, nil
}

type categoryWriter struct {
	encoder     writeStatFlushCloser
	writeBuffer *bufio.Writer // just reusing buffer for effective writes

	eventsBuffer []*categoryEvent // storing all buffered events until flush
	settings     categorySettings
	size         uint64

	baseDir        string
	categoryPath   string
	chunk          *chunkInfo
	trafficManager *traffic.Manager
}

func (w *categoryWriter) isEmpty() bool {
	return w.size == 0
}

func (w *categoryWriter) isTimeToRotate() bool {
	return w.size >= w.settings.maxFileSize
}

func (w *categoryWriter) reset() error {

	if w.encoder != nil {
		err := w.encoder.Close()
		if err != nil {
			return fmt.Errorf("failed to close previous file encoders: %v", err)
		}
	}
	currentFileName := w.categoryPath + "/" + w.chunk.makeFilename(w.settings.getExtension())
	fp, err := os.OpenFile(
		currentFileName,
		os.O_APPEND|os.O_CREATE|os.O_WRONLY,
		0666,
	)
	if err != nil {
		return fmt.Errorf("failed to open %s: %v", currentFileName, err)
	}
	err = w.createCurrentSymlink()
	if err != nil {
		return fmt.Errorf("failed to create current symlink: %v", err)
	}

	w.encoder = w.settings.getEncoder(fp)
	st, err := w.encoder.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat %s: %v", currentFileName, err)
	}
	w.size = uint64(st.Size())
	w.writeBuffer.Reset(w.encoder)
	return nil
}

func (w *categoryWriter) rotate() error {
	err := w.flush()
	if err != nil {
		return fmt.Errorf("failed to flush before rotate: %v", err)
	}
	w.chunk.increment()
	err = w.reset()
	if err != nil {
		return fmt.Errorf("failed to reset after rotate: %v", err)
	}
	return nil
}

func (w *categoryWriter) write(event *categoryEvent) (bool, error) {

	w.eventsBuffer = append(w.eventsBuffer, event)

	lines := event.GetLines()
	for i := range lines {
		w.size += uint64(len([]byte(lines[i])))
	}

	if len(w.eventsBuffer) < cap(w.eventsBuffer) && !w.isTimeToRotate() {
		// just queue event, nothing else
		return false, nil
	}
	// we need this flush to update actual file size,
	// because we might have compressed writer
	// and size of buffered lines may differ from size of these lines, written to disk
	// only after it we can check actual file size before rotate
	err := w.flush()
	if err != nil {
		return false, fmt.Errorf("failed to flush on full buffer: %v", err)
	}
	if !w.isTimeToRotate() {
		return false, nil
	}
	err = w.rotate()
	if err != nil {
		return false, fmt.Errorf("failed to rotate after write: %v", err)
	}
	return true, nil
}

func (w *categoryWriter) flush() error {

	if len(w.eventsBuffer) == 0 {
		return nil
	}

	for _, event := range w.eventsBuffer {
		for _, line := range event.GetLines() {
			if len(line) == 0 {
				// client can send empty line sometimes
				continue
			}
			if line[len(line)-1] != '\n' {
				log.Errorf("Incomplete line: '%s'", line)
			}
			_, err := w.writeBuffer.Write([]byte(line))
			if err != nil {
				return fmt.Errorf("failed to write line: %v", err)
			}
		}
	}

	err := w.writeBuffer.Flush()
	if err != nil {
		return fmt.Errorf("failed to flush write buffer: %v", err)
	}
	err = w.encoder.Flush()
	if err != nil {
		return fmt.Errorf("failed to flush encoder: %v", err)
	}

	trafficEvents := make([]*lsd.RequestNewEventsEventT, 0, len(w.eventsBuffer))
	for _, event := range w.eventsBuffer {
		event.answerCh <- &lsd.ResponseOffsetsOffsetT{Inode: event.Inode, Offset: event.Offset}
		trafficEvents = append(trafficEvents, event.RequestNewEventsEventT)
	}
	w.trafficManager.Update(trafficEvents)
	w.eventsBuffer = w.eventsBuffer[:0]

	if !w.settings.gzip {
		return nil
	}
	// we need to update file size because of compressed writer
	st, err := w.encoder.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat file after write to measure final size: %v", err)
	}
	w.size = uint64(st.Size())
	return nil
}

func (w *categoryWriter) close() {

	// discard all queued events
	for _, event := range w.eventsBuffer {
		event.answerCh <- nil
	}
	if w.encoder != nil {
		err := w.encoder.Close()
		if err != nil {
			log.Errorf("failed to close encoder: %v", err)
		}
	}
}

func (w *categoryWriter) createCurrentSymlink() error {

	currentLinkFileName := w.chunk.category + "_current"
	currentLinkNameTmp := w.baseDir + "/" + currentLinkFileName + ".tmp"

	err := os.Remove(currentLinkNameTmp)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to unlink tmp symlink: %v", err)
	}
	err = os.Symlink(
		w.categoryPath+"/"+w.chunk.makeFilename(w.settings.getExtension()),
		currentLinkNameTmp,
	)
	if err != nil {
		return fmt.Errorf("failed to create tmp symlink: %v", err)
	}
	err = os.Rename(currentLinkNameTmp, w.categoryPath+"/"+currentLinkFileName)
	if err != nil {
		return fmt.Errorf("failed to rename tmp link to actual: %v", err)
	}
	return nil
}
