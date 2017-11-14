package main

import (
	"badoo/_packages/log"
	"badoo/_packages/service"
	"badoo/_packages/service/config"
	"github.com/badoo/lsd/internal/client"
	"github.com/badoo/lsd/internal/client/health"
	"github.com/badoo/lsd/internal/server"
	"github.com/badoo/lsd/internal/server/hdfs"
	"github.com/badoo/lsd/internal/traffic"
	lsdProto "github.com/badoo/lsd/proto"
	"os"
)

const COMMAND_HEALTHCHECK = "healthcheck"
const COMMAND_TRANSFER_HDFS = "transfer-hdfs"

var config = &struct {
	badoo_config.ServiceConfig
	lsdProto.LsdConfig
}{}

func main() {

	maybeRunSubCommand()

	// exporting traffic stats
	inTrafficManager := traffic.NewManager("in")
	outTrafficManager := traffic.NewManager("out")

	service.Initialize("conf/lsd.conf", config)
	handler, err := server.NewHandler(config.ServerConfig, inTrafficManager)
	if err != nil {
		log.Fatalf("failed to create handler for lsd server: %v", err)
	}
	defer handler.Shutdown() // graceful shutdown for currently running stuff

	if config.ClientConfig != nil {
		cl, err := client.NewClient(config.ClientConfig, outTrafficManager)
		if err != nil {
			log.Fatalf("failed to create lsd client: %v", err)
		}
		cl.Start()
		defer cl.Stop()
	}
	service.EventLoop([]service.Port{
		service.GpbPort("lsd-gpb", handler, lsdProto.Gpbrpc),
		service.JsonPort("lsd-gpb/json", handler, lsdProto.Gpbrpc),
	})
}

func maybeRunSubCommand() {

	if len(os.Args) < 1 {
		return
	}
	if os.Args[1] != COMMAND_HEALTHCHECK && os.Args[1] != COMMAND_TRANSFER_HDFS {
		return
	}

	// shift "command" argument
	command := os.Args[1]
	os.Args = append(os.Args[:1], os.Args[2:]...)

	switch command {
	case COMMAND_HEALTHCHECK:
		health.Check()
		break
	case COMMAND_TRANSFER_HDFS:
		hdfs.Transfer()
		break
	default:
		return
	}
	os.Exit(0)
}
