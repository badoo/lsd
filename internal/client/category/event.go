package category

import "strings"
import "github.com/badoo/lsd/internal/client/network"

const OLD_FILE_SUFFIX = ".old"
const BIG_FILE_SUFFIX = "_big"
const LOG_FILE_SUFFIX = ".log"

const MAX_BYTES_PER_BATCH = network.MAX_BYTES_TO_SEND / 2 // depends on how much we can to network at once
const MAX_BYTES_PER_LINE = 1 << 20                        // 1mb per line

func NewEvent(cname, absPath string, inode, size uint64, alwaysFlock bool, isFinal bool) Event {
	return Event{
		Name:         cname,
		AbsolutePath: absPath,
		Inode:        inode,
		Size:         size,
		IsRotated:    strings.HasSuffix(absPath, OLD_FILE_SUFFIX),
		IsExclusive: alwaysFlock || strings.HasSuffix(absPath, BIG_FILE_SUFFIX) || // _big
			strings.HasSuffix(absPath, BIG_FILE_SUFFIX+OLD_FILE_SUFFIX) || // _big.old
			strings.HasSuffix(absPath, BIG_FILE_SUFFIX+LOG_FILE_SUFFIX+OLD_FILE_SUFFIX), // _big.log.old,
		IsFinal: isFinal,
	}
}

type Event struct {
	Name         string
	AbsolutePath string
	Inode        uint64
	Size         uint64
	IsExclusive  bool
	IsRotated    bool
	IsFinal      bool
}

type cleanRequest struct {
	absolutePath string
	inode        uint64
}

func (e Event) getOrigFilePath() string {
	return strings.TrimSuffix(e.AbsolutePath, OLD_FILE_SUFFIX)
}

type FinalSyncRequest struct {
	AbsolutePath string
	Inode        uint64
	Size         uint64
}
