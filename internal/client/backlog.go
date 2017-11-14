package client

import "sync"

const BACKLOG_MAX_SIZE = 1000000

func newBacklog() *backlog {
	return &backlog{
		names: make(map[string]bool),
	}
}

type backlog struct {
	sync.Mutex
	names map[string]bool
}

func (b *backlog) add(name string) {
	b.Lock()
	if len(b.names) < BACKLOG_MAX_SIZE {
		b.names[name] = true
	}
	b.Unlock()
}

func (b *backlog) size() int64 {
	b.Lock()
	s := int64(len(b.names))
	b.Unlock()
	return s
}

func (b *backlog) flush() []string {
	res := make([]string, 0, len(b.names))
	b.Lock()
	for name := range b.names {
		res = append(res, name)
	}
	b.names = make(map[string]bool)
	b.Unlock()
	return res
}
