package usage

import (
	"fmt"
	"sync"
)

func NewChecker() *Checker {
	return &Checker{
		freeInoMap: make(map[uint64]bool),
		watching:   make(map[uint64]bool),
		UpdateCh:   make(chan struct{}),
	}
}

type Checker struct {
	resultLock sync.RWMutex
	watchLock  sync.RWMutex

	freeInoMap map[uint64]bool
	watching   map[uint64]bool
	UpdateCh   chan struct{}
}

func (c *Checker) IsFree(ino uint64) bool {
	c.resultLock.RLock()
	res := c.freeInoMap[ino]
	c.resultLock.RUnlock()
	return res
}

func (c *Checker) Watch(ino uint64) {
	c.watchLock.Lock()
	c.watching[ino] = true
	c.watchLock.Unlock()
}

func (c *Checker) Unwatch(ino uint64) {
	c.watchLock.Lock()
	delete(c.watching, ino)
	c.watchLock.Unlock()
}

func (c *Checker) Update() error {

	inoMap := make(map[uint64]bool)
	c.watchLock.RLock()
	for k := range c.watching {
		inoMap[k] = true
	}
	c.watchLock.RUnlock()

	freeInoMap, err := getFreeFiles(inoMap)
	if err != nil {
		return fmt.Errorf("failed to get free files: %v", err)
	}

	c.resultLock.Lock()
	c.freeInoMap = freeInoMap
	c.resultLock.Unlock()

	// we need to replace channel and only then close old one
	// to avoid infinite reads from old closed channel before we replace it
	// two consecutive lines, but too much can happen in the middle
	oldUpdateCh := c.UpdateCh
	c.UpdateCh = make(chan struct{})
	close(oldUpdateCh)

	return nil
}
