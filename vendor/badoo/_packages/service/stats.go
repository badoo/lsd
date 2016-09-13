package service

import (
	"badoo/_packages/gpbrpc"
	"badoo/_packages/service/stats"
	"encoding/json"
	"fmt"
	"net"
	"reflect"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/gogo/protobuf/proto"
)

type StatsCtx struct {
}

var (
	// public variable, is supposed to be set by user (for example in their package's init function)
	// have some defaults, in case user doesn't set it at all
	VersionInfo = badoo_service.ResponseVersion{
		Version:    proto.String("0.0.0"),
		Maintainer: proto.String("NO-USERNAME-SET@corp.badoo.com"),
	}

	stats_ctx = &StatsCtx{}
)

// this function extracts underlying fd from TCPListener
//  CALLER MUST TREAT THIS FD AS READ-ONLY
// the purpose here is to extract real non dup()'d fd for use in GetsockoptTCPInfo()
func getRealFdFromTCPListener(l *net.TCPListener) uintptr {
	file := reflect.ValueOf(l).Elem().FieldByName("fd").Elem()
	return uintptr(file.FieldByName("sysfd").Int())
}

// these functions should be moved somewhere to util package? ... someday when we need it
func Getrusage(who int) (*syscall.Rusage, error) {
	rusage := &syscall.Rusage{}
	err := syscall.Getrusage(who, rusage)
	return rusage, err
}

func timevalToFloat32(tv *syscall.Timeval) float32 {
	return float32(tv.Sec) + float32(tv.Usec)/float32(1000*1000)
}

func GatherServiceStats() (*badoo_service.ResponseStats, error) {
	ru, err := Getrusage(syscall.RUSAGE_SELF)
	if nil != err {
		return nil, fmt.Errorf("getrusage: %v", err)
	}

	// ports stats first
	ports := make([]*badoo_service.ResponseStatsPortStats, len(StartedServers))

	i := 0
	total_connections := uint32(0)
	for _, srv := range StartedServers {
		port_stats := &badoo_service.ResponseStatsPortStats{}

		stats := srv.Server.Stats

		// listen queue information
		unacked, sacked, err := GetLqInfo(srv)
		if err == nil {
			port_stats.LqCur = proto.Uint32(unacked)
			port_stats.LqMax = proto.Uint32(sacked)
		}

		port_connections := atomic.LoadUint64(&stats.ConnCur)
		total_connections += uint32(port_connections)

		// general stats
		port_stats.Proto = proto.String(srv.Name)
		port_stats.Address = proto.String(srv.Address)
		port_stats.ConnCur = proto.Uint64(port_connections)
		port_stats.ConnTotal = proto.Uint64(atomic.LoadUint64(&stats.ConnTotal))
		port_stats.Requests = proto.Uint64(atomic.LoadUint64(&stats.Requests))
		port_stats.BytesRead = proto.Uint64(atomic.LoadUint64(&stats.BytesRead))
		port_stats.BytesWritten = proto.Uint64(atomic.LoadUint64(&stats.BytesWritten))

		// per request stats
		port_stats.RequestStats = make([]*badoo_service.ResponseStatsPortStatsRequestStatsT, 0, len(badoo_service.RequestMsgid_name))
		for msg_id, msg_name := range srv.Server.Proto.GetRequestIdToNameMap() {
			port_stats.RequestStats = append(port_stats.RequestStats, &badoo_service.ResponseStatsPortStatsRequestStatsT{
				Name:  proto.String(msg_name),
				Count: proto.Uint64(atomic.LoadUint64(&stats.RequestsIdStat[msg_id])),
			})
		}

		ports[i] = port_stats
		i++
	}

	r := &badoo_service.ResponseStats{
		Uptime: proto.Uint32(uint32(time.Since(GetStartupTime()).Seconds())),
		RusageSelf: &badoo_service.ResponseStatsRusage{
			RuUtime:   proto.Float32(timevalToFloat32(&ru.Utime)),
			RuStime:   proto.Float32(timevalToFloat32(&ru.Stime)),
			RuMaxrss:  proto.Uint64(uint64(ru.Maxrss)),
			RuMinflt:  proto.Uint64(uint64(ru.Minflt)),
			RuMajflt:  proto.Uint64(uint64(ru.Majflt)),
			RuInblock: proto.Uint64(uint64(ru.Inblock)),
			RuOublock: proto.Uint64(uint64(ru.Oublock)),
			RuNvcsw:   proto.Uint64(uint64(ru.Nvcsw)),
			RuNivcsw:  proto.Uint64(uint64(ru.Nivcsw)),
		},
		Ports:             ports,
		Connections:       proto.Uint32(total_connections),
		InitPhaseDuration: proto.Uint32(uint32(GetInitPhaseDuration().Seconds())),
	}

	return r, nil
}

func (s *StatsCtx) RequestStats(rctx gpbrpc.RequestT, request *badoo_service.RequestStats) gpbrpc.ResultT {
	stats, err := GatherServiceStats()
	if err != nil {
		return badoo_service.Gpbrpc.ErrorGeneric(err.Error())
	}

	return gpbrpc.Result(stats)
}

func (s *StatsCtx) RequestVersion(rctx gpbrpc.RequestT, request *badoo_service.RequestVersion) gpbrpc.ResultT {
	return gpbrpc.Result(&VersionInfo)
}

func (s *StatsCtx) RequestConfigJson(rctx gpbrpc.RequestT, request *badoo_service.RequestConfigJson) gpbrpc.ResultT {
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
