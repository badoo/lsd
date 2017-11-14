// +build linux

package netutil

import (
	"fmt"
	"net"
	"syscall"
	"unsafe"
)

func GetsockoptTCPInfo(fd uintptr) (*syscall.TCPInfo, error) {
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

func GetListenQueueInfo(l net.Listener) (unacked, sacked uint32, err error) {

	fd, err := GetRealFdFromListener(l)
	if err != nil {
		return 0, 0, fmt.Errorf("can't get fd from listener: %v", err)
	}

	tcpinfo, err := GetsockoptTCPInfo(uintptr(fd))
	if err != nil {
		return
	}

	unacked = tcpinfo.Unacked
	sacked = tcpinfo.Sacked

	return
}
