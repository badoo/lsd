package gpbrpc

import (
	"badoo/_packages/log"
	"badoo/_packages/pinba"

	"encoding/json"
	"net"
	"os"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/gogo/protobuf/proto"
)

const (
	MaxMsgID              = 64 // should be enough for everybody, FIXME: this needs to be dynamic, obviously
	SleepAfterAcceptError = 500 * time.Millisecond
)

var (
	GlobalRequestId = uint64(0)
)

// ---------------------------------------------------------------------------------------------------------------

type Server interface {
	Listener() net.Listener

	Stats() *ServerStats
	Protocol() Protocol

	Serve()
	Stop() error
	StopGraceful() error
}

// tcp server specific
type ServerCodec interface {
	ReadRequest(Protocol, net.Conn) (msgid uint32, msg proto.Message, nread int, status ConnStatus, err error)
	WriteResponse(Protocol, net.Conn, proto.Message) (nwritten int, err error)
}

type ServerStats struct {
	ConnCur        uint64
	ConnTotal      uint64
	Requests       uint64
	BytesRead      uint64
	BytesWritten   uint64
	RequestsIdStat [MaxMsgID]uint64
}

type PinbaSender interface {
	Send(req *pinba.Request) error
}

// ---------------------------------------------------------------------------------------------------------------

type RequestT struct {
	Server    Server
	Conn      net.Conn      // FIXME: might be null (zmq has no 'connection', should change to an interface with remote addr, etc)
	RequestId uint64        // globally unique id for this request, useful in matching log entries belonging to single request
	MessageId uint32        // request_msgid enum value from protofile
	Message   proto.Message // parsed message
	PinbaReq  *pinba.Request
}

type ActionT int

const (
	ACTION_RESULT = iota
	ACTION_SUSPEND
	ACTION_CLOSE
	ACTION_NO_RESULT
)

type ResultT struct {
	Action  ActionT
	Message proto.Message
}

func Result(message proto.Message) ResultT {
	return ResultT{Action: ACTION_RESULT, Message: message}
}

func ResultClose() ResultT {
	return ResultT{Action: ACTION_CLOSE}
}

// FIXME: this should be removed probably? what's the use for it anyway? (this is not event-driven libangel)
func ResultSuspend() ResultT {
	return ResultT{Action: ACTION_SUSPEND}
}

func ResultNoResult() ResultT {
	return ResultT{Action: ACTION_NO_RESULT}
}

// ---------------------------------------------------------------------------------------------------------------

// FIXME: RequestT args in methods here? name sucks as well
//
type connectWrapperChecker interface {
	OnConnect(RequestT)
	OnDisconnect(RequestT)
}

// basic server implementation, intended to be embedded by actual implementations (like tcpServer)
// needs to be 'public' for zmqServer to work (since it's in a different package, to make cgo linking optional)
type ServerImpl struct {
	L               net.Listener
	Proto           Protocol
	Handler         interface{}
	S               ServerStats
	PinbaReqNames   map[uint32]string // cached msgid -> pinba_request_name map
	PinbaSender     PinbaSender
	SlowRequestTime time.Duration
}

func (server *ServerImpl) Stats() *ServerStats {
	return &server.S
}

func (server *ServerImpl) Listener() net.Listener {
	return server.L
}

func (server *ServerImpl) Protocol() Protocol {
	return server.Proto
}

func (server *ServerImpl) HandleRequest(req RequestT) ResultT {
	atomic.AddUint64(&server.S.Requests, 1)

	if req.MessageId < MaxMsgID { // FIXME: make this dynamic
		atomic.AddUint64(&server.S.RequestsIdStat[req.MessageId], 1)
	}

	startTime := time.Now() // use monotonic clock for timing things

	result := server.Proto.Dispatch(req, server.Handler)

	requestTime := time.Since(startTime)

	// log slow request if needed
	if server.SlowRequestTime > 0 && server.SlowRequestTime <= requestTime {
		reqName := MapRequestIdToName(server.Proto, req.MessageId)
		reqBody := func() string {
			if req.Message == nil {
				return "{}"
			}
			body, err := json.Marshal(req.Message)
			if err != nil {
				return err.Error()
			}
			return string(body)
		}()

		if req.Conn != nil {
			log.Warnf("slow request (%d ms): %s %s; addr: %s <- %s",
				requestTime/time.Millisecond, reqName, reqBody, req.Conn.LocalAddr(), req.Conn.RemoteAddr())
		} else {
			log.Warnf("slow request (%d ms): %s %s; no conn;",
				requestTime/time.Millisecond, reqName, reqBody)
		}
	}

	server.SendToPinba(req.PinbaReq, server.PinbaReqNames[req.MessageId], requestTime)

	return result
}

func (server *ServerImpl) SendToPinba(req *pinba.Request, script_name string, duration time.Duration) {

	if server.PinbaSender == nil {
		return
	}

	req.ScriptName = script_name
	req.RequestCount = 1
	req.RequestTime = duration

	err := server.PinbaSender.Send(req)
	if err != nil { // this is pretty unlikely, therefore don't care about performance inside this 'if'

		shouldLog := func() bool {
			opErr, ok := err.(*net.OpError)
			if !ok {
				return true
			}

			syscallErr, ok := opErr.Err.(*os.SyscallError)
			if !ok {
				return true
			}

			errno, ok := syscallErr.Err.(syscall.Errno)
			if !ok {
				return true
			}

			if errno == syscall.ECONNREFUSED {
				// NOTE(antoxa): see CT-2394
				// just ignore connection refused errors, those happen when pinba is unavailable, but configured (since we use UDP)
				// we kinda lose on error reporting that nothing is logged to pinba, but in practice the message is just annoying
				return false
			}

			return true
		}()

		if shouldLog {
			log.Warnf("SendToPinba() failed: %v", err)
		}
	}
}
