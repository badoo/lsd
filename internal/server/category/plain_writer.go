package category

import (
	"badoo/_packages/log"
	"github.com/badoo/lsd/internal/traffic"
	"github.com/badoo/lsd/proto"
	"bufio"
	"fmt"
	"os"
)

type Event struct {
	*lsd.RequestNewEventsEventT
	AnswerCh chan *lsd.ResponseOffsetsOffsetT
}

type Writer interface {
	Prepare() error
	Write(*Event) (bool, error) // (rotated after write, error)
	Rotate() error
	Flush() error
	Close() error
}

func NewPlainWriter(cname, cpath string, settings Settings, trafficManager *traffic.Manager) *PlainWriter {
	return &PlainWriter{
		writeBuffer:    bufio.NewWriterSize(nil, WRITE_BUFFER_SIZE),
		settings:       settings,
		categoryPath:   cpath,
		categoryName:   cname,
		trafficManager: trafficManager,
	}
}

type PlainWriter struct {
	eventsBuffer   []*Event // storing all buffered events until flush
	encoder        writeStatFlushCloser
	writeBuffer    *bufio.Writer // just reusing buffer for effective writes
	settings       Settings
	categoryPath   string
	categoryName   string
	trafficManager *traffic.Manager
}

func (w *PlainWriter) Prepare() error {
	return w.reset()
}

func (w *PlainWriter) Write(event *Event) (bool, error) {

	w.eventsBuffer = append(w.eventsBuffer, event)
	if len(w.eventsBuffer) < cap(w.eventsBuffer) {
		// just queue event, nothing else
		return false, nil
	}
	// flush buffered changes to disk
	err := w.Flush()
	if err != nil {
		return false, fmt.Errorf("failed to flush on full buffer: %v", err)
	}
	return false, nil
}

func (w *PlainWriter) Rotate() error {

	err := w.Flush()
	if err != nil {
		return fmt.Errorf("failed to flush before rotate: %v", err)
	}
	err = w.reset()
	if err != nil {
		return fmt.Errorf("failed to reset after rotate: %v", err)
	}
	return nil
}

func (w *PlainWriter) Flush() error {

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
		event.AnswerCh <- &lsd.ResponseOffsetsOffsetT{Inode: event.Inode, Offset: event.Offset}
		trafficEvents = append(trafficEvents, event.RequestNewEventsEventT)
	}
	w.trafficManager.Update(trafficEvents)
	w.eventsBuffer = w.eventsBuffer[:0]
	return nil
}

func (w *PlainWriter) Close() error {
	// discard all queued events
	for _, event := range w.eventsBuffer {
		event.AnswerCh <- nil
	}
	if w.encoder != nil {
		return w.encoder.Close()
	}
	return nil
}

func (w *PlainWriter) reset() error {

	if w.encoder != nil {
		err := w.encoder.Close()
		if err != nil {
			return fmt.Errorf("failed to close previous file encoders: %v", err)
		}
	}
	currentFileName := w.categoryPath + w.settings.getExtension()
	fp, err := os.OpenFile(
		currentFileName,
		os.O_APPEND|os.O_CREATE|os.O_WRONLY,
		0666,
	)
	if err != nil {
		return fmt.Errorf("failed to open %s: %v", currentFileName, err)
	}
	w.encoder = w.settings.getEncoder(fp)
	w.writeBuffer.Reset(w.encoder)
	return nil
}
