package lsd

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

// WriterPlain is used for plain text files (e.x. logs and etc.)
// it sends raw lines as is (with mandatory \n termination symbol)
// long line (>4k) can be corrupted/mixed with other
// it writes events into <BaseDir>/<category>.log (file is rotated by LSD client)
type WriterPlain struct {
	BaseDir  string
	Category string
}

func (w *WriterPlain) WriteString(eventLine string) error {
	return w.WriteBytes([]byte(eventLine))
}

func (w *WriterPlain) WriteJson(data interface{}) error {

	eventBytes, err := json.Marshal(data)
	if err != nil {
		return err
	}
	return w.WriteBytes(eventBytes)
}

func (w *WriterPlain) WriteBytes(eventBytes []byte) error {

	eventBytes = append(bytes.TrimRight(eventBytes, "\n"), '\n')
	return doWrite(
		fmt.Sprintf(
			"%s/%s.log",
			strings.TrimRight(w.BaseDir, "/"),
			w.Category,
		),
		os.O_CREATE|os.O_WRONLY|os.O_APPEND,
		eventBytes,
		false,
	)
}

func (w *WriterPlain) Write(p []byte) (int, error) {

	// @TODO fair bytes written value
	err := w.WriteBytes(p)
	if err != nil {
		return 0, err
	}
	return len(p), nil
}
