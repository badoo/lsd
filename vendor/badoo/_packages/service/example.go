package service

/*

Sample copy-pasted from experimental cryptod

package main

import (
	"badoo/_packages/gpbrpc"
	"badoo/_packages/log"
	"badoo/_packages/service"
	"badoo/_packages/service/config"
	"badoo/cryptod/conf"
	"badoo/cryptod/proto"
	"github.com/gogo/protobuf/proto"
)

type FullConfig struct {
	badoo_config.ServiceConfig
	badoo_cryptod.CryptodConfig
}

type cryptod_gpb struct {
}

func (c *cryptod_gpb) RequestTest(conn *gpbrpc.ServerConn, request *cryptod.RequestTest) proto.Message {
	return cryptod.Gpbrpc.OK("all gewd")
}

func main() {
	var config = FullConfig{}
	// var config = badoo_cryptod.CryptodConfig{}

	var gpb = cryptod_gpb{}

	var ports = []service.Port{
		service.GpbPort("cryptod/gpbs", &gpb, cryptod.Gpbrpc),
		service.JsonPort("cryptod/json", &gpb, cryptod.Gpbrpc),
	}

	service.Initialize("conf/cryptod.conf", &config)

	log.Debugf("flags = %# v\n", *service.CmdlineFlags())
	log.Debugf("config = %# v\n", config)
	// log.Debugf("config = %# v\n", config.GetDaemonConf())

	service.EventLoop(ports)
}
*/
