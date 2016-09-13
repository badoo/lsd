package dns

import (
	"badoo/_packages/log"
	"badoo/_packages/util"
	"fmt"
	"net"
	"sync"
	"time"
)

type Resolver interface {
	LookupHost(host string) ([]string, error)
	LookupHostEx(host string) (ips []string, cached bool, err error)
	LookupHostPort(hostport string) ([]string, error) // antoxa: temporary, for gpbrpc.Client
	Stop()
}

type cacheItem struct {
	host string
	ips  []string
	err  error
}

type resolveReq struct {
	host      string
	responseC chan<- cacheItem
}

type resolver struct {
	resolveInterval time.Duration
	logger          *log.Logger

	cacheLock sync.RWMutex
	cache     map[string]cacheItem
	resolveC  chan resolveReq
	quitC     chan struct{}
	stopped   bool
}

var (
	std = NewResolver(30*time.Second, log.StandardLogger()) // default resolver, no logger

	ErrStopped = fmt.Errorf("Resolver is stopped")
)

func (r *resolver) LookupHost(host string) ([]string, error) {
	ips, _, err := r.LookupHostEx(host)
	return ips, err
}

func (r *resolver) LookupHostEx(host string) (ips []string, cached bool, err error) {
	if r.stopped {
		return nil, false, ErrStopped
	}

	r.cacheLock.RLock()
	item, ok := r.cache[host]
	r.cacheLock.RUnlock() // unlock early!

	if ok {
		return item.ips, true, nil
	}

	// cache miss - slowpath

	result := func() cacheItem {
		resultC := make(chan cacheItem, 1)
		defer close(resultC)

		r.resolveC <- resolveReq{host, resultC}
		return <-resultC
	}()

	return result.ips, false, result.err
}

func (r *resolver) LookupHostPort(hostport string) ([]string, error) {
	if r.stopped { // detect stopped early
		return nil, ErrStopped
	}

	host, _, err := net.SplitHostPort(hostport)
	if err != nil {
		return nil, err
	}

	return r.LookupHost(host)
}

func (r *resolver) Stop() {
	if !r.stopped {
		close(r.quitC)
		r.stopped = true
	}
}

func (r *resolver) resolveLoop() {
	for {
		select {
		case req := <-r.resolveC:

			// check cache again, in case we've just resolved that hostname
			// i.e. imagine service starts up, cache is empty and multiple goroutines all request to resolve the same hostname
			r.cacheLock.RLock()
			item, exists := r.cache[req.host]
			r.cacheLock.RUnlock()

			if exists {
				req.responseC <- item
			} else {
				item, _ := r.resolveAndUpdate(cacheItem{host: req.host})
				req.responseC <- item
			}

		case <-r.quitC:
			return
		}
	}
}

func (r *resolver) updateLoop() {
	ticker := time.NewTicker(r.resolveInterval)
	defer ticker.Stop()

	copyCacheItems := func() []cacheItem {
		r.cacheLock.RLock()
		defer r.cacheLock.RUnlock()

		items := make([]cacheItem, 0, len(r.cache))
		for _, item := range r.cache {
			items = append(items, item)
		}

		return items
	}

	for {
		select {
		case <-ticker.C:

			items := copyCacheItems()

			// TODO(antoxa): maybe do this in parallel
			for _, item := range items {
				r.resolveAndUpdate(item)
			}

		case <-r.quitC:
			return
		}
	}
}

func (r *resolver) resolveAndUpdate(item cacheItem) (cacheItem, error) {
	ips, err := net.LookupHost(item.host)
	if err != nil {
		if r.logger != nil {
			r.logger.Warnf("resolveAndUpdate %v: %v", item.host, err)
		}

		r.cacheLock.Lock()
		delete(r.cache, item.host)
		r.cacheLock.Unlock()

		return cacheItem{err: err}, err
	}

	if item.ips == nil || !util.StrSliceEqual(item.ips, ips) {
		if r.logger != nil {
			r.logger.Debugf("resolveAndUpdate: %s ips changed %v -> %v", item.host, item.ips, ips)
		}

		item.ips = ips
		item.err = err

		r.cacheLock.Lock()
		r.cache[item.host] = item
		r.cacheLock.Unlock()
	}

	return item, nil
}

func NewResolver(resolveInterval time.Duration, logger *log.Logger) Resolver {
	r := &resolver{
		resolveInterval: resolveInterval,
		logger:          logger,
		cache:           make(map[string]cacheItem),
		resolveC:        make(chan resolveReq, 100), // arbitrary buffer length
		quitC:           make(chan struct{}),
	}
	go r.resolveLoop()
	go r.updateLoop()
	return r
}

func StdResolver() Resolver {
	return std
}

func LookupHost(host string) ([]string, error) {
	return std.LookupHost(host)
}

func LookupHostEx(host string) ([]string, bool, error) {
	return std.LookupHostEx(host)
}

func LookupHostPort(hostport string) ([]string, error) {
	return std.LookupHostPort(hostport)
}
