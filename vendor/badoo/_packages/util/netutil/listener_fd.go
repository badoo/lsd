package netutil

import (
	"errors"
	"net"
	"reflect"
)

// GetRealFdFromListener extracts underlying fd from Listener
// returns: extracted fd and error if any
//  CALLER MUST TREAT THIS FD AS READ-ONLY
// the purpose here is to extract real non dup()'d fd (the one returned by TCPListener::File)
func GetRealFdFromListener(l net.Listener) (int64, error) {
	if tcp_l, ok := l.(*net.TCPListener); ok {
		file := reflect.ValueOf(tcp_l).Elem().FieldByName("fd").Elem()

		if sysfd := file.FieldByName("sysfd"); sysfd.IsValid() { // before 1.9.0
			return sysfd.Int(), nil
		}

		if pfd := file.FieldByName("pfd"); pfd.IsValid() { // 1.9.0+
			return pfd.FieldByName("Sysfd").Int(), nil
		}
	}

	return -1, errors.New("GetRealFdFromListener supports TCP listeners only")
}
