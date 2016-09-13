package pinba

import (
	"badoo/_packages/dns"
	"badoo/_packages/pinba/proto"

	"net"
	"sync"
	"time"
)

type Client struct {
	address string
	conn    net.Conn // cached connection

	// resolver recheck
	host string // store to avoid splitting host:port for every request
	port string
	ip   string // resolved remote ip that we use for the conn
}

type Timer struct {
	Tags map[string]string

	// private stuff for simpler api
	stopped  bool
	started  time.Time
	duration time.Duration
}

type Request struct {
	Hostname     string
	ServerName   string
	ScriptName   string
	RequestCount uint32
	RequestTime  time.Duration
	DocumentSize uint32
	MemoryPeak   uint32
	Utime        float32
	Stime        float32
	timers       []Timer
	Status       uint32
	lk           sync.Mutex
}

type GPBRequest struct {
	Pinba.Request
}

func iN(haystack []string, needle string) (int, bool) {
	for i, s := range haystack {
		if s == needle {
			return i, true
		}
	}
	return -1, false
}

func (req *GPBRequest) preallocateArrays(timers []Timer) {

	// calculate (max) final lengths for all arrays
	nTimers := 0
	nTags := 0
	for _, timer := range timers {
		nTimers += 1
		nTags += len(timer.Tags)
	}

	// construct arrays capable of holding all possible values to reduce allocations
	req.TimerHitCount = make([]uint32, 0, nTimers) // number of hits for each timer
	req.TimerValue = make([]float32, 0, nTimers)   // timer value for each timer
	req.Dictionary = make([]string, 0, nTags)      // all strings used in timer tag names/values
	req.TimerTagCount = make([]uint32, 0, nTimers) // number of tags for each timer
	req.TimerTagName = make([]uint32, 0, nTags)    // flat array of all tag names (as offsets into dictionary) laid out sequentially for all timers
	req.TimerTagValue = make([]uint32, 0, nTags)   // flat array of all tag values (as offsets into dictionary) laid out sequentially for all timers
}

func (req *GPBRequest) mergeTags(tags map[string]string) {

	req.TimerTagCount = append(req.TimerTagCount, uint32(len(tags)))

	for k, v := range tags {
		{
			pos, exists := iN(req.Dictionary, k)
			if !exists {
				req.Dictionary = append(req.Dictionary, k)
				pos = len(req.Dictionary) - 1
			}

			req.TimerTagName = append(req.TimerTagName, uint32(pos))
		}

		{
			pos, exists := iN(req.Dictionary, v)
			if !exists {
				req.Dictionary = append(req.Dictionary, v)
				pos = len(req.Dictionary) - 1
			}

			req.TimerTagValue = append(req.TimerTagValue, uint32(pos))
		}
	}
}

func NewClient(address string) (*Client, error) {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return nil, err
	}

	pc := &Client{
		address: address,
		host:    host,
		port:    port,
	}

	return pc, nil
}

func (pc *Client) doConnect(ip string) error {
	if pc.conn != nil {
		pc.conn.Close()
	}

	conn, err := net.Dial("udp", net.JoinHostPort(ip, pc.port))
	if err != nil {
		return err
	}

	pc.ip = ip
	pc.conn = conn

	return nil
}

// NOTE(antoxa): this function is NOT safe to use from multiple goroutines
func (pc *Client) reconnect() error {
	ips, err := dns.LookupHost(pc.host)
	if err != nil {
		return err
	}

	if pc.conn == nil { // first connect
		return pc.doConnect(ips[0])
	}

	// validate that newly resolved ips contain our current address
	_, in := iN(ips, pc.ip)

	// if not found -> reopen connection
	if !in {
		err = pc.doConnect(ips[0])
		if err != nil {
			return err
		}
	}

	return nil
}

// NOTE(antoxa): this function is NOT safe to use from multiple goroutines
func (pc *Client) SendRequest(request *Request) error {

	pbreq := GPBRequest{
		Request: Pinba.Request{
			Hostname:     request.Hostname,
			ServerName:   request.ServerName,
			ScriptName:   request.ScriptName,
			RequestCount: request.RequestCount,
			RequestTime:  float32(request.RequestTime.Seconds()),
			DocumentSize: request.DocumentSize,
			MemoryPeak:   request.MemoryPeak,
			RuUtime:      request.Utime,
			RuStime:      request.Stime,
			Status:       request.Status,
		},
	}

	pbreq.preallocateArrays(request.timers)

	for _, timer := range request.timers {
		pbreq.TimerHitCount = append(pbreq.TimerHitCount, 1)
		pbreq.TimerValue = append(pbreq.TimerValue, float32(timer.duration.Seconds()))
		pbreq.mergeTags(timer.Tags)
	}

	buf := make([]byte, pbreq.Size())
	n, err := pbreq.MarshalTo(buf)
	if err != nil {
		return err
	}

	err = pc.reconnect()
	if err != nil {
		return err
	}

	_, err = pc.conn.Write(buf[:n])
	if err != nil {
		// just in case - drop the connection on write error (will reconnect next request)
		// TODO(antoxa): maybe write to log ?
		pc.conn.Close()
		pc.conn = nil

		return err
	}

	return nil
}

func (req *Request) AddTimer(timer *Timer) {
	req.lk.Lock()
	defer req.lk.Unlock()

	req.timers = append(req.timers, *timer)
}

// this is exactly the same as AddTimer
//  exists only to have api naming similar to pinba php extension
func (req *Request) TimerAdd(timer *Timer) {
	timer.Stop()
	req.AddTimer(timer)
}

func TimerStart(tags map[string]string) *Timer {
	return &Timer{
		duration: 0,
		Tags:     tags,
		stopped:  false,
		started:  time.Now(),
	}
}

func NewTimer(tags map[string]string, duration time.Duration) *Timer {
	return &Timer{
		duration: duration,
		Tags:     tags,
		stopped:  true,
		started:  time.Now().Add(-duration),
	}
}

func (t *Timer) Stop() {
	if !t.stopped {
		t.stopped = true
		t.duration = time.Now().Sub(t.started)
	}
}

func (t *Timer) GetDuration() time.Duration {
	return t.duration
}
