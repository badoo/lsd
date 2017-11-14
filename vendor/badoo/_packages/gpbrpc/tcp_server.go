package gpbrpc

import (
	"badoo/_packages/log"
	"badoo/_packages/pinba"

	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gogo/protobuf/proto"
)

type TCPServerConfig struct {
	Name            string
	Proto           Protocol
	Codec           ServerCodec
	PinbaSender     PinbaSender
	SlowRequestTime time.Duration
}

type tcpServer struct {
	ServerImpl

	Codec         ServerCodec
	onConnect     func(RequestT)
	onDisconnect  func(RequestT)
	connectionsWg sync.WaitGroup // wait for all per-connection goroutines to exit
	ctx           context.Context
	ctxCancel     context.CancelFunc
}

func NewTCPServer(l net.Listener, handler interface{}, config TCPServerConfig) *tcpServer {
	s := &tcpServer{
		ServerImpl: ServerImpl{
			L:               l,
			Proto:           config.Proto,
			Handler:         handler,
			S:               ServerStats{},
			PinbaReqNames:   MakeRequestIdToPinbaNameMap(config.Proto),
			PinbaSender:     config.PinbaSender,
			SlowRequestTime: config.SlowRequestTime,
		},
		Codec: config.Codec,
	}

	// FIXME(antoxa): connectWrapperChecker needs a rename, badly!
	if m, ok := handler.(connectWrapperChecker); ok {
		s.onConnect = m.OnConnect
		s.onDisconnect = m.OnDisconnect
	}

	s.ctx, s.ctxCancel = context.WithCancel(context.Background())

	return s
}

func (server *tcpServer) Serve() {
	for {
		conn, err := server.Listener().Accept()
		if err != nil {

			if strings.Index(err.Error(), "use of closed network connection") != -1 {
				// this error happens after we've called listener.Close() in other goroutine
				return
			}

			log.Errorf("accept() failed: \"%s\", will sleep for %v before trying again", err, SleepAfterAcceptError)
			time.Sleep(SleepAfterAcceptError)

			continue
		}

		server.connectionsWg.Add(1) // do NOT move this to serveConnection()
		go server.serveConnection(conn)
	}
}

func (server *tcpServer) Stop() error {
	return server.Listener().Close()
}

func (server *tcpServer) StopGraceful() error {
	err := server.Stop()
	if err != nil {
		return err
	}

	server.ctxCancel()          // ask all goroutines to exit
	server.connectionsWg.Wait() // and wait till they do

	return nil
}

func (server *tcpServer) serveConnection(conn net.Conn) {
	log.Debugf("accepted connection: %s -> %s", conn.LocalAddr(), conn.RemoteAddr())

	atomic.AddUint64(&server.S.ConnCur, 1)
	atomic.AddUint64(&server.S.ConnTotal, 1)

	defer func() {
		atomic.AddUint64(&server.S.ConnCur, ^uint64(0)) // decrements the value, ref: http://golang.org/pkg/sync/atomic/#AddUint64
		log.Debugf("closing connection: %s -> %s", conn.LocalAddr(), conn.RemoteAddr())

		conn.Close()                // this signals network io goroutine to exit
		server.connectionsWg.Done() // tells server that we've exited
	}()

	// special connect/disconnect handlers
	// these always run on the same goroutine
	//  - onConnect handler
	//  - onShutdown handler
	//  - request handlers
	{
		if server.onConnect != nil {
			server.onConnect(RequestT{Server: server, Conn: conn})
		}

		defer func() {
			if server.onDisconnect != nil {
				server.onDisconnect(RequestT{Server: server, Conn: conn})
			}
		}()
	}

	// start network io in a separate goroutine, it'll exit when conn.Close() is called
	// will also close `requestChan` when exiting
	requestChan, resultChan := server.runConnectionNetworkIO(conn)
	defer close(resultChan)

	// process requests, die either
	//  - on cancel (server shutdown, maybe will add connection close from other conns/requests)
	//  - when incoming channel is closed (conn close)
	for {
		select {
		case <-server.ctx.Done():
			// notify network io to close if it's waiting for response
			// (can happen if we've got shutdown right after request has been read from network)
			resultChan <- ResultClose()
			return

		case nr, ok := <-requestChan:
			if !ok { // network chan closed, just die
				return
			}

			req := RequestT{
				Server:    server,
				Conn:      conn,
				RequestId: atomic.AddUint64(&GlobalRequestId, 1),
				MessageId: nr.msgid,
				Message:   nr.msg,
				PinbaReq:  &pinba.Request{}, // if you wanna cache - make pinbaReq.Clean() function or something
			}

			result := server.HandleRequest(req)

			resultChan <- result
		}
	}
}

type netRequest struct {
	msgid uint32
	msg   proto.Message
}

func (server *tcpServer) runConnectionNetworkIO(conn net.Conn) (<-chan netRequest, chan<- ResultT) {
	// handler goroutine will receive requests on this channel
	// network goroutine will close it, when network connection is closed
	// buffer is important, as network must always be able to write!
	requestChan := make(chan netRequest, 1)

	// handler goroutine will send results on this channel
	// handler goroutine will send ResultClose() on shutdown request
	// network goroutine will die after
	// buffer is important, as handler must always be able to write!
	resultChan := make(chan ResultT, 1)

	go server.serveConnectionNetworkIO(conn, requestChan, resultChan)
	return requestChan, resultChan
}

// serveConnectionNetworkIO handles all network-related stuff for a given connection
// terminates when conn is closed (from other goroutine, never closes it by itself)
// when conn is closed - closes requestChan to signal it's completion
func (server *tcpServer) serveConnectionNetworkIO(conn net.Conn, requestChan chan<- netRequest, resultChan <-chan ResultT) {
	defer close(requestChan) // this signals request processing goroutine that we're done here

	var result ResultT // FIXME: this is here only because of goto write_response

	for {
		msgid, msg, bytes_read, status, err := server.Codec.ReadRequest(server.Proto, conn)

		atomic.AddUint64(&server.S.BytesRead, uint64(bytes_read)) // may have read some bytes, regardless of error

		if err != nil {
			// TODO: get rid of status, replace it with custom-made errors

			if status == ConnEOF { // connection was closed gracefully from client side
				return
			}

			if status == ConnIOError { // IO error or malformed packet (data stream potentially inconsistent)
				log.Infof("aborting connection %s -> %s: %v", conn.LocalAddr(), conn.RemoteAddr(), err)
				return
			}

			if status == ConnParseError { // misunderstanding on protobuf level, but can use the connection
				result = server.Proto.ErrorGeneric(err)

				// FIXME(antoxa): remove this goto, it's annoying to code around
				goto write_response
			}

			// status == ConnOK && err != nil -> this must not happen
			// you've either added new Conn* status without handling it, or your codec implementation is bugged
			panic(fmt.Sprintf("must NOT be reached, conn status: %d, err: %v", status, err))
		}

		// ask for request to be executed
		requestChan <- netRequest{msgid, msg}

		// wait for execution (i.e. don't read from the network!)
		// IT'S CRITICAL TO WAIT HERE, BEFORE READING NEXT REQUEST, OR THIS GOROUTINE MIGHT LEAK
		// EXPLANATION: if there is no wait - consider this
		//  1. request is read, submitted for execution
		//  2. there is no wait for request to be executed (i.e. code goes to read more immediately)
		//  3. another request is read and we're now blocked on sending to `requestChan`
		//  4. request handler goroutine - decides that it's time to close the connection (due to server shutdown)
		//  5. request handler goroutine - stops reading from this channel - and we hang forever
		// POSSIBLE SOLUTIONS
		//  1. wait here, like this one
		//  2. close(requestChan) from other goroutine, we panic+recover here and die
		//  3. additional channel to die here (too many fucking channels already)
		//  4. make requestChan buffered and never send more than buf_size-1 messages (no easy way to implement i can see)
		result = <-resultChan

	write_response:

		if result.Action == ACTION_NO_RESULT {
			continue
		}

		if result.Message != nil { // got a response to send
			writeln, err := server.Codec.WriteResponse(server.Proto, conn, result.Message)

			atomic.AddUint64(&server.S.BytesWritten, uint64(writeln)) // may have written some bytes before error

			if err != nil {
				// write error: can't recover
				log.Infof("unrecoverable error while writing %s -> %s: %v", conn.LocalAddr(), conn.RemoteAddr(), err)
				return
			}
		}

		if result.Action == ACTION_CLOSE {
			return
		}
	}
}
