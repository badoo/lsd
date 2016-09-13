package common

import (
	"sync"
)

// do not allow more than maxThreads concurrent IO operations
func IoManagerThread(maxThreads int, ioRequestChan chan chan bool, ioFreeChan chan bool) {
	freeThreads := maxThreads // free threads count

	for {
		reqCh := ioRequestChan
		freeCh := ioFreeChan

		if freeThreads <= 0 {
			reqCh = nil
		}

		select {
		case respChan := <-reqCh:
			freeThreads--
			respChan <- true
		case <-freeCh:
			freeThreads++
		}
	}
}

func RequestIO(ioRequestChan chan chan bool) {
	respChan := make(chan bool)
	ioRequestChan <- respChan
	<-respChan
}

func ReleaseIO(ioFreeChan chan bool) {
	ioFreeChan <- true
}

func CloneStatMap(source map[string]int64, mutex *sync.Mutex) map[string]int64 {
	mutex.Lock()
	defer mutex.Unlock()

	res := make(map[string]int64)
	for k, v := range source {
		res[k] = v
	}

	return res
}
