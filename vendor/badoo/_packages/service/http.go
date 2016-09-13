package service

import (
	"badoo/_packages/log"

	"fmt"
	stdlog "log"
	"net"
	"os"
	"strings"
	"time"

	"net/http"
	_ "net/http/pprof"

	"github.com/gorilla/handlers"
)

type httpLoggerWrapper struct {
}

func (l *httpLoggerWrapper) Write(data []byte) (int, error) {
	log.Errorf("[http pprof] %s", strings.Trim(string(data), " \t\r\n"))
	return len(data), nil
}

type httpServer struct {
	Addr     string
	Listener net.Listener

	server *http.Server
}

func (s *httpServer) Serve() {
	s.server = &http.Server{
		Addr:           s.Addr,
		Handler:        handlers.CompressHandler(http.DefaultServeMux),
		ReadTimeout:    10 * time.Second,
		WriteTimeout:   10 * time.Second,
		MaxHeaderBytes: 1 << 20,
		ErrorLog:       stdlog.New(&httpLoggerWrapper{}, "", 0),
	}
	s.server.Serve(s.Listener)
}

func getHttpRestartSocket(rcd *RestartChildData, addr string) (*RestartSocket, *os.File) {
	if rcd == nil {
		return nil, nil
	}

	restartSocket := rcd.HttpPProfSocket
	if restartSocket.Address == "" {
		// parent doesn't have http_pprof_address
		return nil, nil
	}

	restartFile := os.NewFile(restartSocket.Fd, "")

	if restartSocket.Address != addr {
		return nil, restartFile
	}

	return &restartSocket, restartFile
}

// start http/pprof and expvar server
// FIXME: refactor graceful restart copy-paste stuff here
func newHttpServer(config Config, rcd *RestartChildData) (*httpServer, error) {
	var err error

	daemonConfig := config.GetDaemonConfig()

	addr := daemonConfig.GetHttpPprofAddr()
	if addr == "" {
		return nil, nil
	}

	restartSocket, restartFile := getHttpRestartSocket(rcd, addr)
	defer func() {
		if restartFile != nil {
			restartFile.Close()
		}
	}()

	var listener net.Listener
	if restartSocket == nil {
		listener, err = net.Listen("tcp", addr)
		if err != nil {
			return nil, fmt.Errorf("listen failed for server http_pprof at %s: %s", addr, err)
		}

		log.Infof("port http_pprof_addr bound to address %s", addr)
	} else {
		listener, err = net.FileListener(restartFile) // this dup()-s
		if err != nil {
			return nil, fmt.Errorf("failed to grab parent fd %d for http_pprof_addr at %s: %s", restartSocket.Fd, addr, err)
		}
		log.Infof("http_pprof_addr bound to address %s (parent fd: %d)", listener.Addr(), restartSocket.Fd)
	}

	server := &httpServer{
		Addr:     addr,
		Listener: listener,
	}

	return server, nil
}
