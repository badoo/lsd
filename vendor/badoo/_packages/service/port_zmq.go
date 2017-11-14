// +build zmq_enabled

package service

import (
	"badoo/_packages/gpbrpc"
	"badoo/_packages/gpbrpc/zmq"
	"badoo/_packages/service/stats"

	"fmt"
	"net"
	"regexp"
)

// EnableZmqPorts enables zeromq support in go-badoo for your service
// this file is only built when compiling go-badoo with zmq support (see below)
// this is done to avoid having every developer install zmq on their machine
//
// zeromq 4.2 is required, lower versions won't work
//
// cd $GOPATH/src/badoo
// make WITH_ZMQ=/path/to/zeromq/version-4.2
func EnableZmqPorts() {
	ports = append(ports, ZmqPort("service-stats-gpb/zmq", statsCtx, badoo_service.Gpbrpc))
}

func ZmqPort(name string, handler interface{}, rpc gpbrpc.Protocol) Port {
	return Port{
		Name:      name,
		Handler:   handler,
		Proto:     rpc,
		IsStarted: false,

		Listen: func(network, address string) (net.Listener, error) {

			// parse + verify zmq address
			addrParts, err := func() ([]string, error) {
				reString := `^(tcp|udp)://(.*?):([0-9]{1,5})`
				re := regexp.MustCompile(reString)
				addrParts := re.FindAllStringSubmatch(address, -1)

				if len(addrParts) != 1 || len(addrParts[0]) != 4 { // 4 == full string + 3 submatches
					return nil, fmt.Errorf("bad zmq address: %s (expected to match %q)", address, reString)
				}

				return addrParts[0], nil
			}()

			if err != nil {
				return nil, err
			}

			// addrParts = [full_address, tcp_or_udp, hostname, port]

			protocol, host, port := addrParts[1], addrParts[2], addrParts[3]
			if protocol == "udp" {
				return nil, fmt.Errorf("udp networks are not supported for zmq")
			}

			return net.Listen(network, fmt.Sprintf("%s:%s", host, port))
		},

		NewServer: func(l net.Listener, p *Port) (gpbrpc.Server, error) {
			return gpbrpc_zmq.NewZmqServer(l, p.Handler, gpbrpc_zmq.ZmqServerConfig{
				ZmqAddress:          p.ConfAddress,
				Proto:               rpc,
				PinbaSender:         p.PinbaSender,
				SlowRequestTime:     p.SlowRequestTime,
				MaxParallelRequests: p.MaxParallelRequests,
			})
		},
	}
}
