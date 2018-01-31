package client

import (
	"badoo/_packages/log"
	"github.com/badoo/lsd/internal/client/offsets"
	"github.com/badoo/lsd/internal/traffic"
	lsdProto "github.com/badoo/lsd/proto"
	"fmt"
	"sync"
	"time"
)

func NewClient(config *lsdProto.LsdConfigClientConfigT, trafficManager *traffic.Manager) (*Client, error) {

	offsetsDb, err := offsets.InitDb(config.GetOffsetsDb())
	if err != nil {
		return nil, fmt.Errorf("failed to load offsetsDb db from disk: %v", err)
	}
	if offsetsDb.IsEmpty() {
		log.Warning("loaded offsets db is empty")
	}

	netRouter, err := NewNetworkRouter(offsetsDb, trafficManager, config)
	if err != nil {
		return nil, fmt.Errorf("failed to init network router: %v", err)
	}

	fsRouter, err := newFsRouter(config, trafficManager, netRouter, offsetsDb)
	if err != nil {
		return nil, fmt.Errorf("failed to init fs router: %v", err)
	}
	return &Client{
		config:         config,
		offsetsDb:      offsetsDb,
		trafficManager: trafficManager,
		fsRouter:       fsRouter,
		netRouter:      netRouter,
	}, nil
}

type Client struct {
	config *lsdProto.LsdConfigClientConfigT

	offsetsDb      *offsets.Db
	trafficManager *traffic.Manager
	fsRouter       *fsRouter
	netRouter      *NetworkRouter
}

func (c *Client) Start() {

	// calculate, update and publish categories traffic stats
	c.trafficManager.Publish()
	go c.updateTrafficStatsLoop()

	// save offsets db to disk
	go c.saveOffsetsLoop()

	c.fsRouter.start()
}

func (c *Client) Stop() {

	// can stop all in parallel
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		c.fsRouter.stop()
		wg.Done()
	}()
	wg.Add(1)
	go func() {
		c.netRouter.stop()
		wg.Done()
	}()
	wg.Wait()

	// force offsets db save before exit
	err := c.offsetsDb.Save()
	if err != nil {
		log.Errorf("failed to save offsets on stop: %v", err)
	}
}

func (c *Client) updateTrafficStatsLoop() {
	ticker := time.Tick(int2sec(c.config.GetTrafficStatsRecalcInterval()))
	for {
		<-ticker
		c.trafficManager.Recalculate(c.config.GetTrafficStatsRecalcInterval())
	}
}

func (c *Client) saveOffsetsLoop() {
	ticker := time.Tick(int2sec(c.config.GetOffsetsSaveInterval()))
	for {
		<-ticker
		err := c.offsetsDb.Save()
		if err != nil {
			log.Fatalf("failed to save offsets: %v", err)
		}
	}
}
