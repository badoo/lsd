package service

import (
	"badoo/_packages/gpbrpc"
	"badoo/_packages/log"
	"badoo/_packages/service/stats"
	"badoo/_packages/util/netutil"

	"encoding/json"
	"fmt"
	"os"
	"sync/atomic"
	"syscall"
	"time"

	"badoo/_packages/util"

	"github.com/gogo/protobuf/proto"
)

type statsContext struct{}

var (
	// VersionInfo is a public variable. It is supposed to be set by user (for example in their package's init function)
	// have some defaults, in case user doesn't set it at all
	VersionInfo = badoo_service.ResponseVersion{
		Version:    proto.String("0.0.0"),
		Maintainer: proto.String("NO-USERNAME-SET@corp.badoo.com"),
	}

	statsCtx = &statsContext{}
)

// these functions should be moved somewhere to util package? ... someday when we need it
func getrusage(who int) (*syscall.Rusage, error) {
	rusage := &syscall.Rusage{}
	err := syscall.Getrusage(who, rusage)
	return rusage, err
}

func gatherServiceStats() (*badoo_service.ResponseStats, error) {
	ru, err := getrusage(syscall.RUSAGE_SELF)
	if nil != err {
		return nil, fmt.Errorf("getrusage: %v", err)
	}

	// ports stats first
	ports := make([]*badoo_service.ResponseStatsPortStats, len(startedServers))

	i := 0
	totalConnections := uint32(0)
	for _, srv := range startedServers {
		portStats := &badoo_service.ResponseStatsPortStats{}

		stats := srv.server.Stats()

		// listen queue information
		unacked, sacked, err := netutil.GetListenQueueInfo(srv.server.Listener())
		if err == nil {
			portStats.LqCur = proto.Uint32(unacked)
			portStats.LqMax = proto.Uint32(sacked)
		}

		portConnections := atomic.LoadUint64(&stats.ConnCur)
		totalConnections += uint32(portConnections)

		// general stats
		portStats.Proto = proto.String(srv.name)
		portStats.Address = proto.String(srv.realAddress)
		portStats.ConnCur = proto.Uint64(portConnections)
		portStats.ConnTotal = proto.Uint64(atomic.LoadUint64(&stats.ConnTotal))
		portStats.Requests = proto.Uint64(atomic.LoadUint64(&stats.Requests))
		portStats.BytesRead = proto.Uint64(atomic.LoadUint64(&stats.BytesRead))
		portStats.BytesWritten = proto.Uint64(atomic.LoadUint64(&stats.BytesWritten))

		// per request stats
		portStats.RequestStats = make([]*badoo_service.ResponseStatsPortStatsRequestStatsT, 0, len(badoo_service.RequestMsgid_name))
		for msgID, msgName := range srv.server.Protocol().GetRequestIdToNameMap() {
			portStats.RequestStats = append(portStats.RequestStats, &badoo_service.ResponseStatsPortStatsRequestStatsT{
				Name:  proto.String(msgName),
				Count: proto.Uint64(atomic.LoadUint64(&stats.RequestsIdStat[msgID])),
			})
		}

		ports[i] = portStats
		i++
	}

	r := &badoo_service.ResponseStats{
		Uptime: proto.Uint32(uint32(time.Since(GetStartupTime()).Seconds())),
		RusageSelf: &badoo_service.ResponseStatsRusage{
			RuUtime:   proto.Float32(util.TimevalToFloat32(&ru.Utime)),
			RuStime:   proto.Float32(util.TimevalToFloat32(&ru.Stime)),
			RuMaxrss:  proto.Uint64(uint64(ru.Maxrss)),
			RuMinflt:  proto.Uint64(uint64(ru.Minflt)),
			RuMajflt:  proto.Uint64(uint64(ru.Majflt)),
			RuInblock: proto.Uint64(uint64(ru.Inblock)),
			RuOublock: proto.Uint64(uint64(ru.Oublock)),
			RuNvcsw:   proto.Uint64(uint64(ru.Nvcsw)),
			RuNivcsw:  proto.Uint64(uint64(ru.Nivcsw)),
		},
		Ports:             ports,
		Connections:       proto.Uint32(totalConnections),
		InitPhaseDuration: proto.Uint32(uint32(GetInitPhaseDuration().Seconds())),
	}

	return r, nil
}

func (s *statsContext) RequestStats(rctx gpbrpc.RequestT, request *badoo_service.RequestStats) gpbrpc.ResultT {
	stats, err := gatherServiceStats()
	if err != nil {
		return badoo_service.Gpbrpc.ErrorGeneric(err.Error())
	}

	return gpbrpc.Result(stats)
}

func (s *statsContext) RequestMemoryStats(rctx gpbrpc.RequestT, request *badoo_service.RequestMemoryStats) gpbrpc.ResultT {
	return badoo_service.Gpbrpc.ErrorGeneric("not implemented in go-badoo")
}

func (s *statsContext) RequestReturnMemoryToOs(rctx gpbrpc.RequestT, request *badoo_service.RequestReturnMemoryToOs) gpbrpc.ResultT {
	return badoo_service.Gpbrpc.ErrorGeneric("not implemented in go-badoo")
}

func (s *statsContext) RequestProcStats(rctx gpbrpc.RequestT, request *badoo_service.RequestProcStats) gpbrpc.ResultT {
	pid := os.Getpid()
	path := fmt.Sprintf("/proc/%d/statm", pid)

	fp, err := os.Open(path)
	if err != nil {
		log.Errorf("request_proc_stats: %v", err)
		return badoo_service.Gpbrpc.ErrorGeneric(fmt.Sprintf("failed to get %s stats", path))
	}
	defer fp.Close()

	var size, resident, shared, text, lib, data, dt int
	_, err = fmt.Fscanf(fp, "%d %d %d %d %d %d %d", &size, &resident, &shared, &text, &lib, &data, &dt)
	if err != nil {
		log.Errorf("request_proc_stats: failed to parse %s: %v", path, err)
		return badoo_service.Gpbrpc.ErrorGeneric(fmt.Sprintf("failed to get %s stats", path))
	}

	PAGE_SIZE := os.Getpagesize()

	return gpbrpc.Result(&badoo_service.ResponseProcStats{
		Size_:    proto.Uint64(uint64(size * PAGE_SIZE)),
		Resident: proto.Uint64(uint64(resident * PAGE_SIZE)),
		Shared:   proto.Uint64(uint64(shared * PAGE_SIZE)),
		Text:     proto.Uint64(uint64(text * PAGE_SIZE)),
		Data:     proto.Uint64(uint64(data * PAGE_SIZE)),
	})
}

func (s *statsContext) RequestVersion(rctx gpbrpc.RequestT, request *badoo_service.RequestVersion) gpbrpc.ResultT {
	return gpbrpc.Result(&VersionInfo)
}

func (s *statsContext) RequestLogNotice(rctx gpbrpc.RequestT, request *badoo_service.RequestLogNotice) gpbrpc.ResultT {
	log.Infof("%q", request.GetText())

	return badoo_service.Gpbrpc.OK()
}

func (s *statsContext) RequestLogSetLevel(rctx gpbrpc.RequestT, request *badoo_service.RequestLogSetLevel) gpbrpc.ResultT {

	oldLevel := log.GetLevel()

	if request.Level != nil {
		var newLevel log.Level

		switch request.GetLevel() {
		case badoo_service.LogLevel_LOG_DEBUG:
			newLevel = log.DebugLevel
		case badoo_service.LogLevel_LOG_NOTICE:
			newLevel = log.InfoLevel
		case badoo_service.LogLevel_LOG_WARNING:
			newLevel = log.WarnLevel
		case badoo_service.LogLevel_LOG_ERROR, badoo_service.LogLevel_LOG_ALERT:
			newLevel = log.ErrorLevel
		default:
			return badoo_service.Gpbrpc.ErrorGeneric(fmt.Sprintf("unknown or unsupported log level %v", request.GetLevel()))
		}

		log.SetLevel(newLevel)
	}

	oldLevelPB, err := func(level log.Level) (*badoo_service.LogLevel, error) {
		var l badoo_service.LogLevel
		switch level {
		case log.DebugLevel:
			l = badoo_service.LogLevel_LOG_DEBUG
		case log.InfoLevel:
			l = badoo_service.LogLevel_LOG_NOTICE
		case log.WarnLevel:
			l = badoo_service.LogLevel_LOG_WARNING
		case log.ErrorLevel:
			l = badoo_service.LogLevel_LOG_ERROR
		case log.FatalLevel:
			l = badoo_service.LogLevel_LOG_ALERT
		default:
			return nil, fmt.Errorf("error while converting internal log_level %v to external one", level)
		}
		return &l, nil
	}(oldLevel)

	if err != nil {
		return badoo_service.Gpbrpc.ErrorGeneric(fmt.Sprintf("%s", err))
	}

	return gpbrpc.Result(&badoo_service.ResponseLogSetLevel{
		Level:    oldLevelPB,
	})
}

func (s *statsContext) RequestConfigJson(rctx gpbrpc.RequestT, request *badoo_service.RequestConfigJson) gpbrpc.ResultT {
	buf, err := json.Marshal(config)
	if err != nil {
		return gpbrpc.Result(&badoo_service.ResponseGeneric{
			ErrorCode: proto.Int32(-int32(badoo_service.Errno_ERRNO_GENERIC)),
			ErrorText: proto.String(fmt.Sprintf("error while marshalling config to json: %s", err)),
		})
	}

	result := badoo_service.ResponseConfigJson{Json: proto.String(string(buf))}

	return gpbrpc.Result(&result)
}
