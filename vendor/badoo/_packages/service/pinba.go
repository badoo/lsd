package service

import (
	"badoo/_packages/pinba"

	"fmt"
	"sync"
)

var (
	pinbaSender *PinbaSender
)

type PinbaInfo struct {
	Address      string
	ServiceName  string
	InstanceName string
}

type PinbaSender struct {
	*PinbaInfo
	clientPool sync.Pool
}

func NewPinbaSender(pi *PinbaInfo) *PinbaSender {
	return &PinbaSender{
		PinbaInfo: pi,
		clientPool: sync.Pool{
			New: func() interface{} {
				// ignoring error here, sucks
				// but there is a check when creating configuration for better error reporting
				c, _ := pinba.NewClient(pi.Address)
				return c
			},
		},
	}
}

func (ps *PinbaSender) Send(req *pinba.Request) error {
	cli := ps.GetClient()
	if cli == nil {
		return nil
	}

	req.Hostname = ps.ServiceName
	req.ServerName = ps.InstanceName

	err := cli.SendRequest(req)

	ps.clientPool.Put(cli) // this might be a frequent operation, save on defer

	return err
}

func (ps *PinbaSender) GetClient() *pinba.Client {
	return ps.clientPool.Get().(*pinba.Client)
}

// process pinba configuration and return internal configuration
func PinbaInfoFromConfig(config Config) (*PinbaInfo, error) {
	daemonConfig := config.GetDaemonConfig()

	pi := &PinbaInfo{
		Address:      daemonConfig.GetPinbaAddress(),
		ServiceName:  daemonConfig.GetServiceName(),
		InstanceName: daemonConfig.GetServiceInstanceName(),
	}

	if pi.Address == "" {
		return nil, fmt.Errorf("pinba_address not set or empty")
	}

	// check that client can be created, for better error reporting
	_, err := pinba.NewClient(pi.Address)
	if err != nil {
		return nil, err
	}

	return pi, nil
}

func IsPinbaConfigured() bool {
	return pinbaSender != nil
}

func GetPinbaSender() *PinbaSender {
	return pinbaSender
}

// send a request to pinba
// will silently drop requests if pinba hs not been configured
func SendToPinba(req *pinba.Request) error {
	if pinbaSender == nil {
		return nil
	}

	return pinbaSender.Send(req)
}

// grab a pinba Client for your own use
// NOTE: client is NOT safe for use from multiple goroutines
func GetPinbaClient() *pinba.Client {
	if pinbaSender == nil {
		return nil
	}

	return pinbaSender.GetClient()
}
