package util

import (
	"badoo/_packages/log"
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
)

const WATCH_RESULTS_BUFFER_SIZE = 100

// Read starts to watch for category's files
// and stream their content to linesCh line by line
// it deletes fully sent files immediately (no confirm mechanics)
// @TODO implement some kind of confirm
// if error occurs, Read exits immediately, closing all background stuff
// read process can be cancelled by cancelling parent ctx
// keep in mind that Read blocks your current goroutine, so you need to use it in separate one
// if you need more efficient read (batching, etc), you can use Watcher along with UnescapeLine func
func Read(ctx context.Context, category Category, linesCh chan []byte) error {

	filesCh := make(chan string, WATCH_RESULTS_BUFFER_SIZE)
	resultCh := make(chan error, 1)

	watcherCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	go func() {
		resultCh <- Watch(watcherCtx, category, filesCh)
	}()

	readBuffer := bufio.NewReader(nil)
	for {
		select {
		case filename := <-filesCh:
			err := readFile(ctx, filename, readBuffer, linesCh)
			if err != nil {
				return fmt.Errorf("reading processing file %s: %v", filename, err)
			}
		case err := <-resultCh:
			if err != nil {
				return fmt.Errorf("watcher internal error: %v", err)
			}
			return nil
		}
	}
}

func readFile(ctx context.Context, filename string, buf *bufio.Reader, linesCh chan []byte) error {

	fp, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("failed to open %s: %v", filename, err)
	}
	defer func() {
		err := fp.Close()
		if err != nil {
			log.Errorf("failed to close %s: %v", filename, err)
		}
	}()

	buf.Reset(fp)
	for {
		lineRaw, err := buf.ReadBytes('\n')
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to read from %s: %v", filename, err)
		}
		line := UnescapeLine(lineRaw)
		select {
		case <-ctx.Done():
			// we shouldn't delete last file if asked to exit
			return nil
		case linesCh <- line:
		}
	}
	err = os.Remove(filename)
	if err != nil {
		return fmt.Errorf("failed to remove %s: %v", filename, err)
	}
	return nil
}
