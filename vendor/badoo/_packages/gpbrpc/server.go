package gpbrpc

import (
	"badoo/_packages/log"
	"badoo/_packages/pinba"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
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
	globalRequestId = uint64(0)
)

type ServerCodec interface {
	ReadRequest(Protocol, net.Conn) (msgid uint32, msg proto.Message, nread int, status ConnStatus, err error)
	WriteResponse(Protocol, net.Conn, proto.Message) (nwritten int, err error)
}

type RequestT struct {
	Server    *Server
	Conn      net.Conn
	RequestId uint64        // globally unique id for this request, useful in matching log entries belonging to single request
	MessageId uint32        // request_msgid enum value from protofile
	Message   proto.Message // parsed message
	PinbaReq  *pinba.Request
}

type ServerStats struct {
	ConnCur        uint64
	ConnTotal      uint64
	Requests       uint64
	BytesRead      uint64
	BytesWritten   uint64
	RequestsIdStat [MaxMsgID]uint64
}

type Server struct {
	Listener        net.Listener
	Codec           ServerCodec
	Proto           Protocol
	Handler         interface{}
	Stats           ServerStats
	pinbaSender     PinbaSender
	pinbaReqNames   map[uint32]string // cached msgid -> pinba_request_name map
	pinbaClientPool sync.Pool         // pinba.Client pool
	onConnect       func(RequestT)
	onDisconnect    func(RequestT)
	slowRequestTime time.Duration
}

type PinbaSender interface {
	Send(req *pinba.Request) error
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

func ResultSuspend() ResultT {
	return ResultT{Action: ACTION_SUSPEND}
}

func ResultNoResult() ResultT {
	return ResultT{Action: ACTION_NO_RESULT}
}

type connectWrapperChecker interface {
	OnConnect(RequestT)
	OnDisconnect(RequestT)
}

func NewServer(l net.Listener, p Protocol, c ServerCodec, handler interface{}, ps PinbaSender, slowRequestTime time.Duration) *Server {
	s := &Server{
		Listener:        l,
		Codec:           c,
		Proto:           p,
		Handler:         handler,
		Stats:           ServerStats{},
		pinbaSender:     ps,
		pinbaReqNames:   MakeRequestIdToPinbaNameMap(p),
		slowRequestTime: slowRequestTime,
	}

	// FIXME(antoxa): connectWrapperChecker needs a rename, badly!
	if m, ok := handler.(connectWrapperChecker); ok {
		s.onConnect = m.OnConnect
		s.onDisconnect = m.OnDisconnect
	}

	return s
}

func (server *Server) Serve() {
	for {
		conn, err := server.Listener.Accept()
		if err != nil {

			if strings.Index(err.Error(), "use of closed network connection") != -1 {
				// this error happens after we've called listener.Close() in other goroutine
				return
			}

			log.Errorf("accept() failed: \"%s\", will sleep for %v before trying again", err, SleepAfterAcceptError)
			time.Sleep(SleepAfterAcceptError)

			continue
		}

		if server.onConnect != nil {
			server.onConnect(RequestT{
				Server: server,
				Conn:   conn,
			})
		}

		go server.serveConnection(conn)
	}
}

func (server *Server) Stop() error {
	return server.Listener.Close()
}

func (server *Server) serveConnection(conn net.Conn) {
	log.Debugf("accepted connection: %s -> %s", conn.LocalAddr(), conn.RemoteAddr())
	defer func() {
		atomic.AddUint64(&server.Stats.ConnCur, ^uint64(0)) // decrements the value, ref: http://golang.org/pkg/sync/atomic/#AddUint64
		log.Debugf("closing connection: %s -> %s", conn.LocalAddr(), conn.RemoteAddr())

		if server.onDisconnect != nil {
			server.onDisconnect(RequestT{
				Server: server,
				Conn:   conn,
			})
		}

		conn.Close()
	}()

	atomic.AddUint64(&server.Stats.ConnCur, 1)
	atomic.AddUint64(&server.Stats.ConnTotal, 1)

	for {
		// need these as var here, since goto might jump over their definition below
		var (
			result      ResultT
			request     RequestT
			startTime   time.Time
			pinbaReq    pinba.Request
			requestTime time.Duration
		)

		msgid, msg, bytes_read, status, err := server.Codec.ReadRequest(server.Proto, conn)
		if err != nil {
			if status == ConnEOF {
				// connection was closed gracefully from client side
				break
			}
			if status == ConnOK {
				// misunderstanding on protobuf level
				result = server.Proto.ErrorGeneric(err)

				// FIXME(antoxa): remove this goto, it's annoying to code around
				goto write_response
			}
			// IO error or similar
			log.Infof("aborting connection: %v", err)
			break
		}

		// FIXME(antoxa): stats will not be incremented when ReadRequest returns err != nil and status == ConnOK
		atomic.AddUint64(&server.Stats.BytesRead, uint64(bytes_read))
		atomic.AddUint64(&server.Stats.Requests, 1)

		request = RequestT{
			Server:    server,
			Conn:      conn,
			RequestId: atomic.AddUint64(&globalRequestId, 1),
			MessageId: msgid,
			Message:   msg,
			PinbaReq:  &pinbaReq,
		}

		if msgid < MaxMsgID {
			atomic.AddUint64(&server.Stats.RequestsIdStat[msgid], 1)
		}

		startTime = time.Now()

		result = server.Proto.Dispatch(request, server.Handler)

		requestTime = time.Since(startTime)

		// log slow request if needed
		if server.slowRequestTime > 0 && server.slowRequestTime <= requestTime {
			if msg != nil {
				requestInfo := func() string {
					reqName := MapRequestIdToName(server.Proto, msgid)
					body, err := json.Marshal(msg)
					if err != nil {
						return fmt.Sprintf("%s %v", reqName, err)
					}
					return fmt.Sprintf("%s %s", reqName, body)
				}()

				log.Warnf("slow request (%d ms): %s; addr: %s <- %s", requestTime/time.Millisecond, requestInfo, conn.LocalAddr(), conn.RemoteAddr())
			}
		}

		server.sendToPinba(&pinbaReq, server.pinbaReqNames[msgid], requestTime)

	write_response:

		if result.Action == ACTION_RESULT {
			writeln, err := server.Codec.WriteResponse(server.Proto, conn, result.Message)
			if err != nil {
				// write error: can't recover
				log.Infof("unrecoverable error while writing: %v", err)
				break
			}
			atomic.AddUint64(&server.Stats.BytesWritten, uint64(writeln))
		}

		if result.Action == ACTION_CLOSE {
			break
		}
	}
}

func (server *Server) sendToPinba(req *pinba.Request, script_name string, duration time.Duration) {
	if server.pinbaSender == nil {
		return
	}

	req.ScriptName = script_name
	req.RequestCount = 1
	req.RequestTime = duration

	err := server.pinbaSender.Send(req)
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
			log.Warnf("sendToPinba() failed: %v", err)
		}
	}
}
