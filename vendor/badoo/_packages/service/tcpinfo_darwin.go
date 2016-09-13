// +build darwin

package service

import "errors"

func GetLqInfo(srv *Server) (unacked, sacked uint32, err error) {
	return 0, 0, errors.New("unsupported on darwin")
}
