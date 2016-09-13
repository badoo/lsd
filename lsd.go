package main

import (
	"badoo/_packages/log"
	"badoo/_packages/service"
	"badoo/_packages/service/config"
	"github.com/badoo/lsd/internal/client"
	"github.com/badoo/lsd/internal/server"
	"github.com/badoo/lsd/proto"
)

type FullConfig struct {
	badoo_config.ServiceConfig
	lsd.LsdConfig
}

var config = FullConfig{}

func main() {
	var gpb = server.GpbContext{}

	var ports = []service.Port{
		service.GpbPort("lsd-gpb", &gpb, lsd.Gpbrpc),
		service.JsonPort("lsd-gpb/json", &gpb, lsd.Gpbrpc),
	}

	service.Initialize("conf/lsd.conf", &config)

	if config.ClientConfig == nil && config.ServerConfig == nil {
		log.Fatalln("Incorrect config: you must define either client_config or server_config or both")
	}

	server.Setup(config.ServerConfig)
	go client.Run(config.ClientConfig)

	service.EventLoop(ports)
}
