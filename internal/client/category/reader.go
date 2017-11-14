package category

import (
	"badoo/_packages/log"
	"bufio"
	"fmt"
	"io"
	"os"
	"syscall"
)

func newEventReader(fp *os.File, isExclusive bool) *eventReader {

	var reader io.Reader = fp
	if isExclusive {
		reader = &exclusiveReader{fp: fp}
	}
	return &eventReader{fileReader: reader, fileName: fp.Name()}
}

type eventReader struct {
	fileName     string
	pendingChunk []byte
	fileReader   io.Reader
}

func (er *eventReader) readBytes(bufReader *bufio.Reader, isFinal bool) ([]byte, error) {

	lineBytes, err := bufReader.ReadBytes('\n')
	if err == nil {
		// all is ok, appending chunk if it exists
		return er.maybePrependChunk(lineBytes), nil
	}
	if err != io.EOF {
		// some generic error
		return lineBytes, err
	}
	// we did hit io.EOF
	if len(lineBytes) > 0 {
		// append this chunk to current pending line part
		er.pendingChunk = append(er.pendingChunk, lineBytes...)
		lineBytes = []byte{}
	}
	if isFinal && er.hasPendingChunk() {
		// force to send pending chunk if we are sure that this event is last
		// and nothing will come further
		res := er.maybePrependChunk(lineBytes)
		log.Errorf("corrupted line found at the end of %s (no terminating \\n). %s", er.fileName, debugLine(res))
		return res, nil
	}
	return []byte{}, io.EOF
}

func (er *eventReader) maybePrependChunk(lineBytes []byte) []byte {
	if er.hasPendingChunk() {
		res := append(er.pendingChunk, lineBytes...)
		er.pendingChunk = []byte{}
		return res
	}
	return lineBytes
}

func (er *eventReader) hasPendingChunk() bool {
	return len(er.pendingChunk) > 0
}

type exclusiveReader struct {
	fp *os.File
}

func (lr *exclusiveReader) Read(p []byte) (int, error) {

	err := syscall.Flock(int(lr.fp.Fd()), syscall.LOCK_SH)
	if err != nil {
		return 0, fmt.Errorf("failed to lock %s: %v", lr.fp.Name(), err)
	}
	bs, err := lr.fp.Read(p)
	unlockErr := syscall.Flock(int(lr.fp.Fd()), syscall.LOCK_UN)
	if unlockErr != nil {
		log.Errorf("failed to unlock %s: %v", lr.fp.Name(), unlockErr)
	}
	return bs, err
}
