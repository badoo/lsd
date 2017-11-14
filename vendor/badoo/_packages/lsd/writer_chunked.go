package lsd

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"
)

// WriterChunked is used for events and data structures streaming
// best way to send events to LSD
// it writes events into <BaseDir>/<category>/<yyyymmddhhmm>.log files
// it also guarantees consistency for long lines (>4k)
// provides escaping of line breaks inside events
// (single event will always be sent as single line)
// for compatibility with badoo php library, we have to use
// analog of PHP's addcslashes($message, "\\\n") for escaping of lines
// so it's not safe to send raw binary data (without any encoding)
type WriterChunked struct {
	BaseDir  string
	Category string
}

func (w *WriterChunked) WriteString(eventLine string) error {
	return w.WriteBytes([]byte(eventLine))
}

func (w *WriterChunked) WriteJson(data interface{}) error {

	eventBytes, err := json.Marshal(data)
	if err != nil {
		return err
	}
	return w.WriteBytes(eventBytes)
}

func (w *WriterChunked) WriteBytes(eventBytes []byte) error {

	eventBytes = append(escapeLine(bytes.TrimRight(eventBytes, "\n")), '\n')

	now := time.Now()
	filenamePrefix := fmt.Sprintf(
		"%s/%s/%04d%02d%02d%02d%02d",
		strings.TrimRight(w.BaseDir, "/"),
		w.Category,
		now.Year(),
		now.Month(),
		now.Day(),
		now.Hour(),
		now.Minute(),
	)

	flags := os.O_CREATE | os.O_WRONLY | os.O_APPEND
	exclusive := false
	if len(eventBytes) > 4096 {
		// after 4k os doesn't guarantee atomic writes
		filenamePrefix += "_big"
		exclusive = true
	}
	return doWrite(filenamePrefix+".log", flags, eventBytes, exclusive)
}

func (w *WriterChunked) Write(p []byte) (int, error) {

	// @TODO fair bytes written value
	err := w.WriteBytes(p)
	if err != nil {
		return 0, err
	}
	return len(p), nil
}

// does same as PHP's addcslashes($message, "\\\n")
// for compatibility with badoo PHP library
// @TODO don't use strings
func escapeLine(eventBytes []byte) []byte {
	return []byte(strings.NewReplacer("\n", "\\n", "\\", "\\\\").Replace(string(eventBytes)))
}

func UnescapeLine(eventBytes []byte) []byte {
	return []byte(strings.NewReplacer("\\n", "\n", "\\\\", "\\").Replace(string(eventBytes)))
}
