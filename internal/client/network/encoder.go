package network

import (
	lsdProto "github.com/badoo/lsd/proto"
	"bytes"
	"compress/gzip"
	"fmt"

	"github.com/gogo/protobuf/proto"
)

const MIN_EVENT_BYTES_TO_COMPRESS = 1024 // only compress events larger than this size

type Encoder interface {
	Encode(*lsdProto.RequestNewEventsEventT) error
}

type EncoderPlain struct{}

func (e *EncoderPlain) Encode(ev *lsdProto.RequestNewEventsEventT) error {
	return nil
}

type EncoderGzip struct{}

// compresses event if enabled and not already compressed
// modifies event in place
func (e *EncoderGzip) Encode(ev *lsdProto.RequestNewEventsEventT) error {

	if ev.GetIsCompressed() {
		return nil
	}

	evenByteSize := 0
	// TODO(antoxa): maybe pass event data size here, to avoid extra calculation
	for _, line := range ev.GetLines() {
		evenByteSize += len(line)
	}
	// do not compress events that are too small
	if evenByteSize < MIN_EVENT_BYTES_TO_COMPRESS {
		return nil
	}

	// TODO(antoxa): keeping this simple for now
	// could maybe use pools here and reuse gzip writer to reduce allocations
	buf := bytes.Buffer{}
	w := gzip.NewWriter(&buf)
	for _, line := range ev.GetLines() {
		_, err := w.Write([]byte(line))
		if err != nil {
			return fmt.Errorf("failed to write to buffer: %v", err)
		}
	}

	if err := w.Close(); err != nil {
		return fmt.Errorf("failed to close buffer: %v", err)
	}

	ev.Lines = []string{buf.String()}
	ev.IsCompressed = proto.Bool(true)
	return nil
}
