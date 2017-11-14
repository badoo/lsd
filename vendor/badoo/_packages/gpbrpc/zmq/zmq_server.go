package gpbrpc_zmq

import (
	"badoo/_packages/gpbrpc"
	"badoo/_packages/log"
	"badoo/_packages/pinba"
	"badoo/_packages/util/netutil"
	"encoding/binary"
	"fmt"
	"net"
	"sync/atomic"
	"syscall"
	"time"

	proto "github.com/gogo/protobuf/proto"
	zmq "github.com/pebbe/zmq4"
)

type ZmqServerConfig struct {
	ZmqAddress          string // zmq-specific addr, as given in config
	Proto               gpbrpc.Protocol
	PinbaSender         gpbrpc.PinbaSender
	SlowRequestTime     time.Duration
	MaxParallelRequests uint
}

type zmqServer struct {
	gpbrpc.ServerImpl
	maxParallelRequests uint
	ctx                 *zmq.Context
	sock                *zmq.Socket
	stopChan            chan struct{}
	gracefulStopChan    chan struct{}
}

func NewZmqServer(l net.Listener, handler interface{}, config ZmqServerConfig) (*zmqServer, error) {
	fd, err := netutil.GetRealFdFromListener(l)
	if err != nil {
		return nil, fmt.Errorf("NewZmqServer: can't get sysfd from listener: %v", err)
	}

	ctx, err := zmq.NewContext()
	if err != nil {
		return nil, fmt.Errorf("NewZmqServer: %v", err)
	}

	sock, err := ctx.NewSocket(zmq.ROUTER)
	if err != nil {
		return nil, err
	}

	err = sock.SetRouterHandover(true)
	if err != nil {
		return nil, fmt.Errorf("NewZmqServer: %v", err)
	}

	err = sock.SetUseFd(int(fd))
	if err != nil {
		return nil, fmt.Errorf("NewZmqServer: %v", err)
	}

	// http://api.zeromq.org/4-2:zmq-setsockopt#toc31
	// NOTE: the file descriptor passed through MUST have been
	// ran through the "bind" and "listen" system calls beforehand.
	// Also, socket option that would normally be passed through zmq_setsockopt like
	// TCP buffers length, IP_TOS or SO_REUSEADDR MUST be set beforehand by the caller,
	// as they must be set before the socket is bound.
	err = sock.Bind(config.ZmqAddress)
	if err != nil {
		return nil, fmt.Errorf("NewZmqServer: %v", err)
	}

	s := &zmqServer{
		ServerImpl: gpbrpc.ServerImpl{
			L:               l,
			Proto:           config.Proto,
			Handler:         handler,
			S:               gpbrpc.ServerStats{},
			PinbaReqNames:   gpbrpc.MakeRequestIdToPinbaNameMap(config.Proto),
			PinbaSender:     config.PinbaSender,
			SlowRequestTime: config.SlowRequestTime,
		},
		maxParallelRequests: config.MaxParallelRequests,
		ctx:                 ctx,
		sock:                sock,
		stopChan:            make(chan struct{}),
		gracefulStopChan:    make(chan struct{}),
	}

	return s, nil
}

// internals

type zmqMessage struct {
	identity []byte
	message  []byte
}

func (server *zmqServer) Serve() {
	var (
		respChan         = make(chan zmqMessage, server.maxParallelRequests*2)
		requestsInFlight uint
		needToStop       bool
	)

	poller := zmq.NewPoller()
	poller.Add(server.sock, zmq.POLLIN)

	// final termination sequence for event loop
	terminateZmq := func() {
		// we close the socket (nothing comes in, nothing comes out)
		server.sock.Close()

		// we close the zmq context
		server.ctx.Term()
		server.ctx = nil

		// and exit
		return
	}

	// make sure terminate is called correctly in this function
	defer func() {
		if server.ctx != nil {
			panic("you should've called terminateZmq() before returning")
		}
	}()

	// main loop
	for {

		// try reading some more requests if we're not stopping and are below parallel requests limit
		// otherwise - just fall through to select statement below and wait for request execution
		if !needToStop && (requestsInFlight < server.maxParallelRequests) {
			// read from the zmq socket
			p, err := poller.Poll(time.Millisecond)
			if err != nil {
				// TODO: maybe use 'socket closed' error here, and not 'context terminated'
				if ze := zmq.AsErrno(err); ze == zmq.ETERM {
					return // this happens when server is stopped (context terminated)
				}

				if zmq.AsErrno(err) == zmq.Errno(syscall.EINTR) {
					continue
				}

				log.Warnf("zmq read error: %v", err)
				continue
			}

			// got something to read
			if len(p) != 0 {
				// this should not block
				zmqMsg, err := server.readZmqMessage()
				if err != nil {
					// TODO: maybe use 'socket closed' error here, and not 'context terminated'
					if ze := zmq.AsErrno(err); ze == zmq.ETERM {
						return // this happens when server is stopped (context terminated)
					}

					log.Warnf("zmq read error: %v", err)
					continue
				}

				requestsInFlight += 1

				go func(zmsg zmqMessage) {
					respChan <- server.execRequest(zmsg)
				}(zmqMsg)
			}
		}

		// wait for worker responses or shutdown
		// this select is guaranteed to finish in 1ms
		select {
		case <-server.stopChan:
			// we are here because client wants us to exit immediately
			// this does NOT leak goroutines that are currently executing requests
			// since response chan is large enough for them to put their responses to
			terminateZmq()
			server.stopChan <- struct{}{}
			return

		case <-server.gracefulStopChan:
			// we are here because client wants us to exit gracefully
			// it means we will reply to all requests currently in flight and then exit

			// we signal others that we are stopping
			needToStop = true

			// after this we expect to read all the responses and write them to network
			// after it's done, we will exit
			// see the code, reading from `respChan` below

		case resp := <-respChan:

			requestsInFlight -= 1

			if err := server.sendZmqMessage(resp); err != nil {
				log.Warnf("zmq send error: %v", err)
			}

			// we were signalled that we have to exit gracefully and
			// all requests have been answered to
			if needToStop && requestsInFlight == 0 {
				terminateZmq()
				server.gracefulStopChan <- struct{}{}
				return
			}

		case <-time.After(1 * time.Millisecond):
			// avoid hanging forever when there are no requests coming in
			break
		}
	}
}

func (server *zmqServer) Stop() error {
	server.stopChan <- struct{}{}
	<-server.stopChan
	return nil
}

func (server *zmqServer) StopGraceful() error {
	server.gracefulStopChan <- struct{}{}
	<-server.gracefulStopChan
	return nil
}

// helpers

func (server *zmqServer) serializeZmqMessage(reqID uint32, msg proto.Message) ([]byte, error) {
	data, err := proto.Marshal(msg)
	if err != nil {
		return nil, err
	}

	resData := make([]byte, len(data)+8)
	binary.BigEndian.PutUint32(resData[:4], server.Proto.GetResponseMsgid(msg))
	binary.BigEndian.PutUint32(resData[4:8], reqID)
	copy(resData[8:], data)

	return resData, nil
}

// this function parses request, executes real request handler and returns the result
// it exists to simplify handling errors, that might occur around request execution
// like parse, message_id and other shit
// this is important, because there is no other way to tell the client that it's doing something wrong
// i.e. without a reply - the client would just wait for a response that is never coming back
func (server *zmqServer) realExecuteRequest(zmsg zmqMessage) (uint32, []byte, error) {
	// NOTE: it's possible to reply here, without req_id
	// but the only thing the client can do - is reset it's request stream and try again
	// client also needs to handle the issue of overlapping req_ids,
	// i.e. some requests might alredy be in progress, and new ones must not have matching req_id's
	// as that would confuse the client beyond measure
	if len(zmsg.message) < 8 {
		return 0, nil, fmt.Errorf("request is too short: %d bytes", len(zmsg.message))
	}

	msgID := binary.BigEndian.Uint32(zmsg.message[:4])
	reqID := binary.BigEndian.Uint32(zmsg.message[4:8])

	// log.Debugf("msg_id: %d, req_id: %d", msg_id, req_id)

	pbMsg := server.Proto.GetRequestMsg(msgID)
	if pbMsg == nil {
		return reqID, nil, fmt.Errorf("unknown message_id: %d", msgID)
	}

	err := proto.Unmarshal(zmsg.message[8:], pbMsg)
	if err != nil {
		return reqID, nil, fmt.Errorf("unmarshal error: %v", err)
	}

	// log.Debugf("pb_msg = %v", pb_msg)
	req := gpbrpc.RequestT{
		Server:    nil,
		Conn:      nil,
		RequestId: atomic.AddUint64(&gpbrpc.GlobalRequestId, 1),
		MessageId: msgID,
		Message:   pbMsg,
		PinbaReq:  &pinba.Request{}, // if you wanna cache - make pinbaReq.Clean() function or something
	}

	res := server.HandleRequest(req)

	data, err := server.serializeZmqMessage(reqID, res.Message)
	if err != nil {
		return reqID, nil, fmt.Errorf("response marshal: %v", err)
	}

	return reqID, data, nil
}

func (server *zmqServer) execRequest(zmsg zmqMessage) zmqMessage {

	// the main execution routine
	// 1. try executing the request and getting something back
	// 2. it if fails - send back a generic response with error text
	//    this is required, since there is no 'connection' in zmq, just messages
	//    and the client needs to know that the server was unable to execute this request
	responseBody := func() []byte {
		reqID, responseBody, err := server.realExecuteRequest(zmsg)
		if err != nil {
			// generic response serialization MUST NOT fail
			res := server.Proto.ErrorGeneric(fmt.Errorf("exec error: %v", err))
			errorBody, e1 := server.serializeZmqMessage(reqID, res.Message)
			if e1 != nil {
				panic("can't serialize response_generic: " + e1.Error())
			}

			return errorBody
		}
		return responseBody
	}()

	return zmqMessage{
		identity: zmsg.identity,
		message:  responseBody,
	}
}

func (server *zmqServer) readZmqMessage() (zmqMessage, error) {
	// get identity
	identity, err := server.sock.RecvBytes(0)
	if err != nil {
		return zmqMessage{}, fmt.Errorf("zmq recv identity error: %v", err)
	}

	// get message
	msg, err := server.sock.RecvBytes(0)
	if err != nil {
		return zmqMessage{}, fmt.Errorf("zmq recv data error: %v", err)
	}

	return zmqMessage{identity, msg}, err
}

func (server *zmqServer) sendZmqMessage(resp zmqMessage) error {
	// send identity
	_, err := server.sock.SendBytes(resp.identity, zmq.SNDMORE)
	if err != nil {
		return fmt.Errorf("can't send identity: %v", err)
	}

	// send data
	_, err = server.sock.SendBytes(resp.message, 0)
	if err != nil {
		return fmt.Errorf("can't send body: %v", err)
	}

	return nil
}
