// +build darwin

package netutil

import (
	"errors"
	"net"
)

func GetListenQueueInfo(l net.Listener) (unacked, sacked uint32, err error) {
	return 0, 0, errors.New("unsupported on darwin")
}
