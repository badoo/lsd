package log_syslog

import (
	"badoo/_packages/log"
	"log/syslog"
	"sync/atomic"
)

// SyslogHook to send logs via syslog.
type SyslogHookNoFrozen struct {
	syslogHook *SyslogHook
	logCh      chan *log.Entry
	exitCh     chan struct{}
	skipped    uint64
}

// Creates a hook to be added to an instance of logger. This is called with
// `hook, err := NewSyslogHookNoFrozen("udp", "localhost:514", syslog.LOG_DEBUG, "")`
func NewSyslogHookNoFrozen(network, raddr string, priority syslog.Priority, tag string) (*SyslogHookNoFrozen, error) {
	syslogHook, err := NewSyslogHook(network, raddr, priority, tag)
	if err != nil {
		return nil, err
	}

	hook := &SyslogHookNoFrozen{
		syslogHook: syslogHook,
		logCh:      make(chan *log.Entry, 1000),
		exitCh:     make(chan struct{}),
	}
	go hook.serve()
	return hook, err
}

func (hook *SyslogHookNoFrozen) Fire(entry *log.Entry) error {
	select {
	case hook.logCh <- entry:
	default:
		skippedNew := atomic.AddUint64(&hook.skipped, uint64(1))
		log.WithoutHooks().Errorf("syslog entry %v skipped (total %d)", entry, skippedNew)
	}
	return nil
}

func (hook *SyslogHookNoFrozen) Levels() []log.Level {
	return hook.syslogHook.Levels()
}

func (hook *SyslogHookNoFrozen) Exit() {
	hook.exitCh <- struct{}{}
}

func (hook *SyslogHookNoFrozen) GetSkippedCount() uint64 {
	return atomic.LoadUint64(&hook.skipped)
}

func (hook *SyslogHookNoFrozen) serve() {
	for {
		select {
		case entry := <-hook.logCh:
			err := hook.syslogHook.Fire(entry)
			if err != nil {
				log.WithoutHooks().Errorf("syslog error: %v", err)
			}
		case <-hook.exitCh:
			return
		}
	}
}
