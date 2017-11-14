package gpbrpc

import (
	"badoo/_packages/dns"
	"badoo/_packages/log"
	"badoo/_packages/util"

	"fmt"
	"net"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
)

type response struct {
	msgid uint32
	body  []byte
	err   error
}

type Client interface {
	Call(req proto.Message) (uint32, proto.Message, error)
	CallRaw(msgid uint32, body []byte) (uint32, []byte, error)
	Address() string
	Close()
	CloseNoReuse()
}

type defaultClient struct {
	address         string
	ips             []string
	conn            net.Conn
	Proto           Protocol
	Codec           ClientCodec
	connect_timeout time.Duration
	request_timeout time.Duration

	lk                   sync.Mutex
	numExpectedResponses int32         // number of requests that we expect responses to
	closed               bool          // conn has been closed, do not reuse this connection
	respch               chan response // written to by readLoop, read by Call*()
}

type ClientCodec interface {
	WriteRequest(Protocol, net.Conn, proto.Message) error
	ReadResponse(Protocol, net.Conn) (uint32, proto.Message, ConnStatus, error)
}

func NewClientCreate(address string, ips []string, p Protocol, c ClientCodec, connect_timeout, request_timeout time.Duration) Client {
	log.Debugf("creating new cli for %s", address)
	return &defaultClient{
		address:         address,
		ips:             ips,
		Proto:           p,
		Codec:           c,
		connect_timeout: connect_timeout,
		request_timeout: request_timeout,
	}
}

func NewClient(address string, p Protocol, c ClientCodec, connect_timeout, request_timeout time.Duration) Client {
	ips, err := dns.LookupHostPort(address)
	if err != nil {
		log.Errorf("dns.LookupHostPort() faield: %v", err)
		// FIXME(antoxa): just reusing existing ips here, which actually sucks
		//                this only works because cli.Call() uses net.Dial() which resolves the name again
	}

	canUseClient := func(client *defaultClient) bool {
		// readLoop() might be modifying this conn
		// but don't really need to lock for ips comparison, since ips are never modified for existing client
		client.lk.Lock()
		defer client.lk.Unlock()

		// TODO(antoxa): can just use one ip for client and recheck not full equality
		//               but only if new ips contain old ip
		if !util.StrSliceEqual(client.ips, ips) {
			return false
		}

		if client.closed {
			return false
		}

		return true
	}

	const max_tries = 3 // arbitrary limit, i know

	for done_tries := 0; done_tries < max_tries; done_tries++ {
		client := Pcm.GetClient(address)
		if client == nil {
			break
		}

		clientTyped := client.(*defaultClient)

		if !canUseClient(clientTyped) {
			clientTyped.closeNoReuse()
			continue
		}

		log.Debugf("reused existing client %p for %s (after %d tries)", client, address, done_tries)
		return clientTyped
	}

	return NewClientCreate(address, ips, p, c, connect_timeout, request_timeout)
}

func (client *defaultClient) Address() string {
	return client.address
}

func (client *defaultClient) establishConnection(network string) error {
	client.lk.Lock()
	defer client.lk.Unlock()

	// conn might've been closed in between Call*() invocations
	if client.closed {
		client.closed = false
		client.conn = nil
	}

	// already established, can reuse
	if client.conn != nil {
		return nil
	}

	// we're creating new connection here; (closed == false && conn == nil)

	// TODO(antoxa): rewrite this to use ips, that we've resolved earlier
	conn, err := net.DialTimeout(network, client.address, client.connect_timeout)
	if err != nil {
		client.closed = true // connection attempt has failed -> do not reuse
		return fmt.Errorf("connect error: %v", err)
	}

	log.Debugf("connected %s -> %s", conn.LocalAddr(), conn.RemoteAddr())

	client.conn = conn
	client.respch = make(chan response, 1) // needs to be buffered, to avoid stalling readLoop() on Write failure
	client.numExpectedResponses = 0        // this is needed to reset counter in case last Write has failed

	go client.readLoop()

	return nil
}

func (client *defaultClient) Close() {
	// XXX: here if Close() here is called (from other goroutine only?) while there are requests being executed
	//       then this client will be reused as normal and might still be in "ok" state
	//       before readLoop() has the chance to kill it.
	//       this IS a race, but same client should never be used from multiple goroutines concurrently
	//       so we don't care about that

	client.lk.Lock()
	defer client.lk.Unlock()

	if !client.closed {
		Pcm.Reuse(client)
	}
}

func (client *defaultClient) CloseNoReuse() {
	client.closeNoReuse()
}

func (client *defaultClient) closeNoReuse() {
	client.lk.Lock()
	defer client.lk.Unlock()

	client.closeNoReuseLocked()
}

func (client *defaultClient) closeNoReuseLocked() {
	if !client.closed {
		client.conn.Close()
		client.closed = true
	}
}

func (client *defaultClient) Call(req proto.Message) (uint32, proto.Message, error) {

	body, err := proto.Marshal(req)
	if err != nil {
		return 0, nil, fmt.Errorf("encode error: %v", err)
	}

	msgid, body, err := client.CallRaw(client.Proto.GetRequestMsgid(req), body)
	if err != nil {
		return 0, nil, err
	}

	msg := client.Proto.GetResponseMsg(msgid)
	if msg == nil {
		return 0, nil, fmt.Errorf("parse error: unknown message_id: %d", msgid)
	}

	err = proto.Unmarshal(body, msg)
	if err != nil {
		msgname := MapResponseIdToName(client.Proto, msgid)
		return 0, nil, fmt.Errorf("parse error: message %s, error: %v", msgname, err)
	}

	return msgid, msg, nil
}

func (client *defaultClient) CallRaw(msgid uint32, body []byte) (uint32, []byte, error) {
	err := client.establishConnection("tcp")
	if err != nil {
		return 0, nil, err
	}

	client.conn.SetDeadline(time.Now().Add(client.request_timeout))

	client.lk.Lock()
	client.numExpectedResponses++
	client.lk.Unlock()

	_, err = WriteRawGpbsPacket(client.conn, msgid, body)
	if err != nil {
		client.closeNoReuse()
		return 0, nil, err
	}

	resp := <-client.respch

	return resp.msgid, resp.body, resp.err
}

func (client *defaultClient) readLoop() {
	for {
		msgid, body, len, status, err := ReadGpbsPacket(client.conn)

		// FIXME(antoxa): add special checking for streaming gpbs responses (we don't support them :))

		client.lk.Lock() // no defer, but keep code in one place and save on creating lambda on every read
		if client.numExpectedResponses == 0 {
			if status == ConnOK {
				log.Errorf("unexpected read: %s -> %s: msgid %d, len: %d", client.conn.RemoteAddr(), client.conn.LocalAddr(), msgid, len)
			} else if status != ConnEOF {
				log.Errorf("error on conn: %s -> %s, %v", client.conn.RemoteAddr(), client.conn.LocalAddr(), err)
			}
			client.closeNoReuseLocked()
			client.lk.Unlock()
			return
		}
		client.lk.Unlock()

		if status != ConnOK {
			client.closeNoReuse() // must be closed before channel communication happens

			client.respch <- response{0, nil, err}
			return
		}

		// do not timeout accidentally on next read/write
		client.conn.SetDeadline(time.Time{})

		// decrement the counter here
		// since otherwise we might read next message immediately and not react to it being unexpected
		client.lk.Lock()
		client.numExpectedResponses--
		client.lk.Unlock()

		client.respch <- response{msgid, body, nil}
	}
}
