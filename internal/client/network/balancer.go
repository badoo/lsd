package network

import (
	"badoo/_packages/log"
	lsdProto "github.com/badoo/lsd/proto"

	"strings"
	"time"

	"container/heap"
	"fmt"

	"github.com/badoo/lsd/internal/client/offsets"
	"github.com/badoo/lsd/internal/traffic"
	"context"
	"sync"

	"github.com/tevino/abool"
)

const (
	UPSTREAM_ERROR_SLEEP_INTERVAL  = time.Second * 60
	BUSY_UPSTREAMS_REPORT_INTERVAL = time.Second * 60
	BUSY_UPSTREAMS_RETRY_INTERVAL  = time.Millisecond * 100
)

func NewBalancer(offsetsDb *offsets.Db, trafficManager *traffic.Manager, outBufferSize uint64, confRouting *lsdProto.LsdConfigClientConfigTRoutingConfigT) *Balancer {

	var encoder Encoder
	if confRouting.GetGzip() {
		encoder = &EncoderGzip{}
	} else {
		encoder = &EncoderPlain{}
	}

	upstreamsCtx, upstreamsCancel := context.WithCancel(context.TODO())

	bufferSize := outBufferSize * confRouting.GetOutBufferSizeMultiplier()
	b := &Balancer{
		offsetsDb:        offsetsDb,
		trafficManager:   trafficManager,
		sentMap:          make(map[uint64]map[uint64]bool),
		sentOffsets:      make(map[uint64]*IntHeap),
		confirmedOffsets: make(map[uint64]*IntHeap),
		InChan:           make(chan *lsdProto.RequestNewEventsEventT, bufferSize), // external channel for events to send
		retryChan:        make(chan *lsdProto.RequestNewEventsEventT, bufferSize), // see b.transferLoop
		sendChan:         make(chan *lsdProto.RequestNewEventsEventT, bufferSize), // sending data to upstreams
		replyChan:        make(chan *lsdProto.ResponseOffsetsOffsetT, bufferSize), // confirming responses from upstreams
		upstreamsCancel:  upstreamsCancel,
	}
	upstreams := make([]*upstream, 0)
	for _, confRcv := range confRouting.Receivers {
		upstreams = append(upstreams, &upstream{
			ctx:            upstreamsCtx,
			dataEncoder:    encoder,
			events:         make(chan *lsdProto.RequestNewEventsEventT, bufferSize),
			successChan:    b.replyChan,
			requeueChan:    b.retryChan,
			addr:           *confRcv.Addr,
			offline:        abool.New(),
			weight:         confRcv.GetWeight(),
			connectTimeout: time.Duration(confRcv.GetConnectTimeout()) * time.Second,
			requestTimeout: time.Duration(confRcv.GetRequestTimeout()) * time.Second,
		})
	}
	normalizeWeights(upstreams)

	b.upstreams = upstreams
	b.resetUpstream()
	return b
}

type Balancer struct {
	upstreams            []*upstream
	currentUpstreamIdx   int
	currentUpstreamScore uint64

	sentMap     map[uint64]map[uint64]bool
	sentOffsets map[uint64]*IntHeap

	confirmedOffsets map[uint64]*IntHeap

	trafficManager *traffic.Manager
	offsetsDb      *offsets.Db

	InChan    chan *lsdProto.RequestNewEventsEventT // incoming events (from fs)
	sendChan  chan *lsdProto.RequestNewEventsEventT // events that are to send to upstreams
	retryChan chan *lsdProto.RequestNewEventsEventT // events that have to be sent in first order (after fail in upstream)
	replyChan chan *lsdProto.ResponseOffsetsOffsetT // committed offsets from upstreams

	upstreamsCancel context.CancelFunc
	upstreamsWg     sync.WaitGroup

	confirmWg sync.WaitGroup
}

func (b *Balancer) Start() {

	for _, ups := range b.upstreams {
		b.upstreamsWg.Add(1)
		go func(u *upstream) {
			b.trafficManager.SetUpstreamStatus(u.addr, true) // just stats to export
			u.sendLoop(b.trafficManager)
			b.upstreamsWg.Done()
		}(ups)
	}
	go b.transferLoop()

	b.confirmWg.Add(1)
	go b.mainLoop(b.offsetsDb)
}

func (b *Balancer) Stop() {

	// wait for all upstreams to stop sending events
	// and returning confirms
	b.upstreamsCancel()
	b.upstreamsWg.Wait()

	// close confirmation channel
	close(b.replyChan)

	// wait for all remain events to be confirmed
	b.confirmWg.Wait()
}

func (b *Balancer) getUpstreamList() string {
	res := ""
	for _, u := range b.upstreams {
		res = res + u.addr + ", "
	}
	return strings.TrimRight(res, ", ")
}

func (b *Balancer) transferLoop() {
	// merge events from InChan and retryChan
	// retryChan should have a priority in order to not to mix events order
	// this goroutine is critical for case when all upstreams are down for a long period:
	// 1) prevents retry events + goroutines leak
	// 2) helps to keep back pressure (no new events from fs are accepted until retry channel is not empty)
	for {
	retriesLoop:
		for {
			select {
			case ev := <-b.retryChan:
				b.sendChan <- ev
			default:
				break retriesLoop
			}
		}
		select {
		case ev, ok := <-b.InChan:
			if !ok {
				return
			}
			b.sendChan <- ev
		case ev := <-b.retryChan:
			b.sendChan <- ev
		}
	}
}

func (b *Balancer) mainLoop(offsetsDb *offsets.Db) {

	lastBusyTs := time.Now()
	busyCnt := 0
	upstreamList := b.getUpstreamList()

	sendCh := b.sendChan
	cleanOldQueuesCh := time.Tick(time.Minute)

	var (
		pendingEvent   *lsdProto.RequestNewEventsEventT
		pendingCheckCh <-chan time.Time
		pendingTicker  *time.Ticker
	)

	for {
		select {

		case ev := <-sendCh:

			// accept all incoming events and pass them to upstreams with round-robin mechanics
			if b.send(ev) {
				b.registerEvent(ev)
				break
			}
			// all upstream are busy
			// don't accept new events until we send pendingEvent
			// but we can't block here, because we need to process replies too
			sendCh = nil
			pendingEvent = ev
			pendingTicker = time.NewTicker(BUSY_UPSTREAMS_RETRY_INTERVAL)
			pendingCheckCh = pendingTicker.C
		case <-pendingCheckCh:

			if b.send(pendingEvent) {
				b.registerEvent(pendingEvent)
				// get back to normal execution flow
				sendCh = b.sendChan
				pendingEvent = nil
				pendingTicker.Stop()
				pendingCheckCh = nil
				break
			}

			now := time.Now()
			busyCnt++

			if now.Sub(lastBusyTs) > BUSY_UPSTREAMS_REPORT_INTERVAL {
				log.Infof(
					"All upstreams busy was %d times during last %d sec for %s",
					busyCnt,
					now.Sub(lastBusyTs)/time.Second,
					upstreamList,
				)
				busyCnt = 0
				lastBusyTs = now
			}
		case resp, ok := <-b.replyChan:

			if !ok {
				b.confirmWg.Done()
				return
			}

			err := b.confirmEvent(offsetsDb, resp)
			if err != nil {
				log.Errorf("failed to confirm event for inode=%d offset=%d: %v", resp.GetInode(), resp.GetOffset(), err)
			}
		case <-cleanOldQueuesCh:
			b.clearEmptyQueues()
		}
	}
}

func (b *Balancer) registerEvent(ev *lsdProto.RequestNewEventsEventT) {

	inode := ev.GetInode()
	offset := ev.GetOffset()

	inodeMap, ok := b.sentMap[inode]
	if !ok {
		inodeMap = make(map[uint64]bool)
		b.sentMap[inode] = inodeMap
	}

	if inodeMap[offset] == true {
		// event has been sent multiple times in case of error
		return
	}

	sentQueue, ok := b.sentOffsets[inode]
	if !ok {
		sentQueue = &IntHeap{}
		heap.Init(sentQueue)
		b.sentOffsets[inode] = sentQueue
	}
	heap.Push(sentQueue, offset)
	inodeMap[offset] = true
}

func (b *Balancer) confirmEvent(offsetsDb *offsets.Db, event *lsdProto.ResponseOffsetsOffsetT) error {

	inode := event.GetInode()
	offset := event.GetOffset()

	// @TODO maybe this check is ambiguous
	inodeMap, ok := b.sentMap[inode]
	if !ok {
		return fmt.Errorf("consistency error: no inode entry in sentMap")
	}

	// @TODO maybe this check is ambiguous
	if inodeMap[offset] == false {
		return fmt.Errorf("consistency error: no offset entry in inodeMap")
	}

	sentHeap, ok := b.sentOffsets[inode]
	// @TODO maybe this check is ambiguous
	if !ok {
		return fmt.Errorf("consistency error: no inode entry in sentOffsets")
	}
	// @TODO maybe this check is ambiguous
	if sentHeap.Len() == 0 {
		return fmt.Errorf("consistency error: sentHeap is empty, but we receive confirm event")
	}

	confirmedHeap, ok := b.confirmedOffsets[inode]
	if !ok {
		confirmedHeap = &IntHeap{}
		heap.Init(confirmedHeap)
		b.confirmedOffsets[inode] = confirmedHeap
	}
	heap.Push(confirmedHeap, offset)

	var lastOffset uint64
	for {
		if (*sentHeap)[0] != (*confirmedHeap)[0] {
			break
		}
		sentOff := heap.Pop(sentHeap).(uint64)
		_ = heap.Pop(confirmedHeap).(uint64) // checked second item above

		delete(inodeMap, sentOff)
		if len(inodeMap) == 0 {
			delete(b.sentMap, inode)
		}
		lastOffset = sentOff

		if confirmedHeap.Len() == 0 {
			break
		}
	}
	if lastOffset == 0 {
		return nil
	}
	err := offsetsDb.SetOffset(inode, lastOffset)
	if err != nil {
		return fmt.Errorf("failed to set (%d, %d) to db: %v", inode, lastOffset, err)
	}
	return nil
}

func (b *Balancer) clearEmptyQueues() {
	for inode := range b.sentOffsets {
		if b.sentOffsets[inode].Len() == 0 {
			delete(b.sentOffsets, inode)
		}
	}
	for inode := range b.confirmedOffsets {
		if b.confirmedOffsets[inode].Len() != 0 {
			continue
		}
		_, ok := b.sentOffsets[inode]
		if !ok {
			// technically it has to be "or sentOffsets[inode].Len() == 0,
			// but we have deleted all empty queues before
			delete(b.confirmedOffsets, inode)
		}
	}
}

func (b *Balancer) send(ev *lsdProto.RequestNewEventsEventT) bool {

	if len(ev.Lines) == 0 {
		return true
	}

	for i := 0; i < len(b.upstreams); i++ {
		rcv := b.currentReceiver()
		if rcv.offline.IsSet() {
			b.nextReceiver()
			continue
		}
		select {
		case rcv.events <- ev:
			// each upstream has weight and score
			// we pick a upstream, it's score = weight
			// than we decrement score on each next pick of this upstream
			// until it's 0, than we pick a next one
			// but if upstream is busy or offline,
			// we have to just skip it, ignoring weights and scores
			b.currentUpstreamScore--
			if b.currentUpstreamScore == 0 {
				b.nextReceiver()
			}
			return true
		default:
			b.nextReceiver()
			continue
		}
	}
	return false
}

func (b *Balancer) currentReceiver() *upstream {
	return b.upstreams[b.currentUpstreamIdx]
}

func (b *Balancer) resetUpstream() {
	b.currentUpstreamIdx = 0
	b.currentUpstreamScore = b.upstreams[b.currentUpstreamIdx].weight
}

func (b *Balancer) nextReceiver() {
	b.currentUpstreamIdx = (b.currentUpstreamIdx + 1) % len(b.upstreams)
	b.currentUpstreamScore = b.upstreams[b.currentUpstreamIdx].weight
}

type offset struct {
	value     uint64
	confirmed bool
}
