package category

import (
	"compress/gzip"
	"os"
	"regexp"

	"io"

	"github.com/badoo/lsd/proto"

	"github.com/klauspost/pgzip"
)

const EXTENSION_GZIP = ".gz"

type Settings struct {
	Mode               lsd.LsdConfigServerConfigTWriteMode
	Patterns           []*regexp.Regexp
	MaxFileSize        uint64
	FileRotateInterval uint64
	Gzip               bool
	GzipParallel       uint64
}

func (s Settings) getExtension() string {
	if s.Gzip {
		return EXTENSION_GZIP
	}
	return ""
}

func (s Settings) getEncoder(fp *os.File) writeStatFlushCloser {

	if !s.Gzip {
		return &plainEncoder{fp}
	}
	// just a normal single thread Gzip
	if s.GzipParallel == 1 {
		return &gzipEncoder{
			fp: fp,
			gz: gzip.NewWriter(fp),
		}
	}
	// parallel Gzip for heavy cases
	gz := pgzip.NewWriter(fp)
	gz.SetConcurrency(int(s.MaxFileSize/s.GzipParallel), int(s.GzipParallel))
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
