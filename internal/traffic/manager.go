package traffic

import (
	lsdProto "github.com/badoo/lsd/proto"
	"expvar"
	"sync"
)

// publishing/updating per category traffic/speed stats
func NewManager(publishPrefix string) *Manager {
	return &Manager{
		publishPrefix: publishPrefix,
		lastStats:     make(map[string]int64),
		totalStats:    make(map[string]int64),
	}
}

type Manager struct {
	sync.Mutex
	publishPrefix string
	published     bool
	lastStats     map[string]int64
	totalStats    map[string]int64
	traffic       *expvar.Map
	speed         *expvar.Map
	upstreams     *expvar.Map
	backlogSize   *expvar.Int
}

func (m *Manager) Update(events []*lsdProto.RequestNewEventsEventT) {
	m.Lock()
	for _, ev := range events {
		for _, ln := range ev.Lines {
			m.totalStats[*ev.Category] += int64(len(ln))
		}
	}
	m.Unlock()
}

func (m *Manager) Publish() {
	if m.published {
		// for tests
		return
	}
	m.traffic = expvar.NewMap(m.publishPrefix + "_traffic")
	m.speed = expvar.NewMap(m.publishPrefix + "_traffic_speed")
	m.upstreams = expvar.NewMap(m.publishPrefix + "_upstreams_status")
	m.backlogSize = expvar.NewInt(m.publishPrefix + "_backlog_size")
	m.published = true
}

func (m *Manager) SetBacklogSize(size int64) {
	m.backlogSize.Set(size)
}

func (m *Manager) SetUpstreamStatus(name string, up bool) {
	v := &expvar.Int{}
	if up {
		v.Set(1)
	}
	m.upstreams.Set(name, v)
}

func (m *Manager) Recalculate(recalcInterval uint64) {

	currentStats := make(map[string]int64)
	speedStats := make(map[string]int64)

	m.Lock()
	for category, traffic := range m.totalStats {
		currentStats[category] = traffic
		speedStats[category] = (traffic - m.lastStats[category]) / int64(recalcInterval)
	}
	m.Unlock()

	updateMap(currentStats, m.traffic)
	updateMap(speedStats, m.speed)
	m.lastStats = currentStats
}

func updateMap(src map[string]int64, dst *expvar.Map) {
	for key, value := range src {
		v := &expvar.Int{}
		v.Add(value)
		// @TODO maybe, we should delete old categories here
		dst.Set(key, v)
	}
}
