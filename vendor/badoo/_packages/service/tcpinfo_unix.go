// +build linux

package service

import (
	"fmt"
	"net"
	"syscall"
	"unsafe"
)

func getsockoptTCPInfo(fd uintptr) (*syscall.TCPInfo, error) {
	var tcpinfo syscall.TCPInfo
	var len = syscall.SizeofTCPInfo
	_, _, err := syscall.Syscall6(
		syscall.SYS_GETSOCKOPT,
		fd,
		syscall.IPPROTO_TCP,
		syscall.TCP_INFO,
		uintptr(unsafe.Pointer(&tcpinfo)),
		uintptr(unsafe.Pointer(&len)),
		0)

	if 0 != err {
		return nil, error(err)
	}

	return &tcpinfo, nil
}

func GetLqInfo(srv *Server) (unacked, sacked uint32, err error) {
	l, ok := srv.Server.Listener.(*net.TCPListener)
	if !ok {
		return 0, 0, fmt.Errorf("undefined for non-tcp listeners")
	}

	tcpinfo, err := getsockoptTCPInfo(getRealFdFromTCPListener(l))
	if err != nil {
		return
	}

	unacked = tcpinfo.Unacked
	sacked = tcpinfo.Sacked

	return
}
