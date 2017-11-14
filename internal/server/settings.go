package server

import (
	"compress/gzip"
	"os"
	"regexp"

	"io"

	"github.com/klauspost/pgzip"
)

const EXTENSION_GZIP = ".gz"

type categorySettings struct {
	patterns           []*regexp.Regexp
	maxFileSize        uint64
	fileRotateInterval uint64
	gzip               bool
	gzipParallel       uint64
}

func (cs *categorySettings) getExtension() string {
	if cs.gzip {
		return EXTENSION_GZIP
	}
	return ""
}

func (cs *categorySettings) getEncoder(fp *os.File) writeStatFlushCloser {

	if !cs.gzip {
		return &plainEncoder{fp}
	}
	// just a normal single thread gzip
	if cs.gzipParallel == 1 {
		return &gzipEncoder{
			fp: fp,
			gz: gzip.NewWriter(fp),
		}
	}
	// parallel gzip for heavy cases
	gz := pgzip.NewWriter(fp)
	gz.SetConcurrency(int(cs.maxFileSize/cs.gzipParallel), int(cs.gzipParallel))
	return &gzipEncoder{fp: fp, gz: gz}
}

// common interface for all encoders
type writeStatFlushCloser interface {
	writeFlushCloser
	Stat() (os.FileInfo, error)
}

type writeFlushCloser interface {
	io.WriteCloser
	Flush() error
}

// just plain file
type plainEncoder struct {
	*os.File
}

func (p *plainEncoder) Flush() error {
	return nil
}

// compressed writer
type gzipEncoder struct {
	gz writeFlushCloser
	fp *os.File
}

func (g *gzipEncoder) Write(p []byte) (int, error) {
	return g.gz.Write(p)
}

func (g *gzipEncoder) Stat() (os.FileInfo, error) {
	return g.fp.Stat()
}

func (g *gzipEncoder) Flush() error {
	return g.gz.Flush()
}

func (g *gzipEncoder) Close() error {
	err := g.gz.Close()
	if err != nil {
		return err
	}
	return g.fp.Close()
}
