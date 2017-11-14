package network

import (
	"badoo/_packages/gpbrpc"
	"badoo/_packages/log"
	lsdProto "github.com/badoo/lsd/proto"
	"fmt"
	"time"

	"github.com/badoo/lsd/internal/traffic"
	"context"

	"github.com/tevino/abool"
)

const (
	MAX_BYTES_TO_SEND    = 1 << 22 // 4mb
	MAX_MESSAGES_TO_SEND = 50      // if batches are big then too many batches could cause OOM eventually
)

type upstream struct {
	ctx         context.Context
	dataEncoder Encoder

	events      chan *lsdProto.RequestNewEventsEventT
	successChan chan *lsdProto.ResponseOffsetsOffsetT
	requeueChan chan *lsdProto.RequestNewEventsEventT

	offline        *abool.AtomicBool
	pendingEvent   *lsdProto.RequestNewEventsEventT
	addr           string
	weight         uint64
	connectTimeout time.Duration
	requestTimeout time.Duration
}

func (u *upstream) accumulateBatch() ([]*lsdProto.RequestNewEventsEventT, bool) {

	evBytes := 0
	events := make([]*lsdProto.RequestNewEventsEventT, 0, MAX_MESSAGES_TO_SEND)

	if u.pendingEvent != nil {
		events = append(events, u.pendingEvent)
		u.pendingEvent = nil
	}

	if len(events) == 0 {
		// we need to perform read in order to block
		// until we have at least one event in batch
		// because len(u.events) can be 0
		ev, ok := u.readEvent(u.events)
		if !ok {
			// asked to exit
			return events, false
		}
		events = append(events, ev)
	}

	// we always have single event in batch here
	evBytes += events[0].Size()

	l := len(u.events)
	for i := 0; i < l; i++ {

		ev, ok := u.readEvent(u.events)
		if !ok {
			// asked to exit
			return events, false
		}

		if evBytes+ev.Size() >= MAX_BYTES_TO_SEND {
			u.pendingEvent = ev
			break
		}
		events = append(events, ev)
		evBytes += ev.Size()

		if len(u.events) == MAX_MESSAGES_TO_SEND {
			break
		}
	}
	return events, true
}

func (u *upstream) readEvent(inCh chan *lsdProto.RequestNewEventsEventT) (*lsdProto.RequestNewEventsEventT, bool) {
	select {
	case ev := <-inCh:
		return ev, true
	case <-u.ctx.Done():
		return nil, false
	}
}

func (u *upstream) sendLoop(trafficManager *traffic.Manager) {

	for {
		batch, ok := u.accumulateBatch()
		if !ok {
			// asked to stop
			return
		}
		ev := &lsdProto.RequestNewEvents{Events: batch}

		resp, err := func() (*lsdProto.ResponseOffsets, error) {

			for _, e := range ev.Events {
				err := u.dataEncoder.Encode(e)
				if err != nil {
					return nil, fmt.Errorf("failed to encode events: %v", err)
				}
			}

			cli := gpbrpc.NewClient(u.addr, &lsdProto.Gpbrpc, &gpbrpc.GpbsCodec, u.connectTimeout, u.requestTimeout)
			_, resp, err := cli.Call(ev)
			cli.Close()

			if err != nil {
				return nil, fmt.Errorf("gpbrpc send call failed: %v", err)
			}

			r, ok := resp.(*lsdProto.ResponseOffsets)
			if !ok {
				return nil, fmt.Errorf("unexpected response from server: %+v", resp)
			}
			err = checkOffsets(ev.Events, r.Offsets)
			if err != nil {
				return nil, fmt.Errorf("invalid offsets received: %v", err)
			}

			return r, nil
		}()

		if err != nil {

			log.Warnf("send to %s, failed: %v", u.addr, err)

			u.offline.SetTo(true)
			log.Infof("server went offline: %s", u.addr)
			trafficManager.SetUpstreamStatus(u.addr, false) // just stats to export

			doneCh := make(chan struct{})
			go func() {
				// return all events from current batch
				for _, e := range ev.Events {
					u.requeueChan <- e
				}
				// return any next event until we went online again
				for {
					select {
					case <-doneCh:
						return
					case e := <-u.events:
						u.requeueChan <- e
					}
				}
			}()

			select {
			case <-u.ctx.Done():
				// asked to stop
				return
			case <-time.After(UPSTREAM_ERROR_SLEEP_INTERVAL):
			}

			close(doneCh)
			u.offline.SetTo(false)
			log.Infof("Server went online: %s", u.addr)
			trafficManager.SetUpstreamStatus(u.addr, true) // just stats to export
			continue
		}

		trafficManager.Update(ev.Events)

		for _, replyRow := range resp.Offsets {
			u.successChan <- replyRow
		}
	}
}

func checkOffsets(events []*lsdProto.RequestNewEventsEventT, offsets []*lsdProto.ResponseOffsetsOffsetT) error {

	requestedOffsets := make(map[uint64]map[uint64]uint64) // inode => (offset => true)
	for _, ev := range events {
		if requestedOffsets[*ev.Inode] == nil {
			requestedOffsets[*ev.Inode] = make(map[uint64]uint64)
		}
		requestedOffsets[*ev.Inode][*ev.Offset]++
	}

	for _, resp := range offsets {
		if requestedOffsets[*resp.Inode] == nil {
			return fmt.Errorf("extra inode #%d received from server which was not present in response", *resp.Inode)
		}

		if _, ok := requestedOffsets[*resp.Inode][*resp.Offset]; !ok {
			return fmt.Errorf("extra offset %d received for inode #%d in response", *resp.Offset, *resp.Inode)
		}

		requestedOffsets[*resp.Inode][*resp.Offset]--
		if requestedOffsets[*resp.Inode][*resp.Offset] <= 0 {
			delete(requestedOffsets[*resp.Inode], *resp.Offset)
		}

		if len(requestedOffsets[*resp.Inode]) == 0 {
			delete(requestedOffsets, *resp.Inode)
		}
	}

	if len(requestedOffsets) > 0 {
		return fmt.Errorf("not all offsets were received from response")
	}
	return nil
}
