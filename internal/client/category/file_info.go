package category

import (
	"badoo/_packages/log"
	"bufio"
	"fmt"
	"io"
	"os"
)

const AVERAGE_LINES_COUNT = 50

type fileInfo struct {
	actualPath string
	isRotated  bool

	pendingLine []byte
	lastOffset  uint64

	fp     *os.File
	reader *eventReader
}

func (fi *fileInfo) readLines(readBuffer *bufio.Reader, isFinal bool) ([]string, error) {

	lines := make([]string, 0, AVERAGE_LINES_COUNT)

	messageSize := 0
	if len(fi.pendingLine) != 0 {
		lines = append(lines, string(fi.pendingLine))
		messageSize = len(fi.pendingLine)
		fi.lastOffset += uint64(len(fi.pendingLine))
		fi.pendingLine = []byte{}
	}

	for {
		lineBytes, err := fi.reader.readBytes(readBuffer, isFinal)
		lineSize := uint64(len(lineBytes))
		if err == io.EOF {
			// we hit io.EOF, nothing to do here
			return lines, nil
		}
		if err != nil {
			return lines, err
		}
		if lineSize > MAX_BYTES_PER_LINE {
			log.Errorf("too large line found (file='%s'; size=%d) - skipping. %s", fi.fp.Name(), lineSize, debugLine(lineBytes))
			fi.lastOffset += lineSize
			// add empty line in order to prevent situation when we have
			// single big line and have nothing to send/commit except it
			lines = append(lines, "")
			continue
		}

		// total lines are bigger that allowed message size
		// postpone current line to next message
		if messageSize+len(lineBytes) > MAX_BYTES_PER_BATCH {
			fi.pendingLine = lineBytes
			return lines, nil
		}

		lines = append(lines, string(lineBytes))
		fi.lastOffset += lineSize
		messageSize += int(lineSize)
	}
}

func debugLine(line []byte) string {
	if len(line) < 1024 {
		return string(line)
	}
	return fmt.Sprintf("[first 1024 bytes]: '%s'", line[:1024])
}
