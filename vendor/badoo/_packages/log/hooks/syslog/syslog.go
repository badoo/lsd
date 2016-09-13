package log_syslog

import (
	"badoo/_packages/log"
	"fmt"
	"log/syslog"
	"os"
)

// SyslogHook to send logs via syslog.
type SyslogHook struct {
	Writer        *syslog.Writer
	SyslogNetwork string
	SyslogRaddr   string
}

// Creates a hook to be added to an instance of logger. This is called with
// `hook, err := NewSyslogHook("udp", "localhost:514", syslog.LOG_DEBUG, "")`
// `if err == nil { log.Hooks.Add(hook) }`
func NewSyslogHook(network, raddr string, priority syslog.Priority, tag string) (*SyslogHook, error) {
	w, err := syslog.Dial(network, raddr, priority, tag)
	return &SyslogHook{w, network, raddr}, err
}

func (hook *SyslogHook) Fire(entry *log.Entry) error {
	line, err := entry.String()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to read entry, %v", err)
		return err
	}

	switch entry.Level {
	case log.PanicLevel:
		return hook.Writer.Crit(line)
	case log.FatalLevel:
		return hook.Writer.Crit(line)
	case log.ErrorLevel:
		return hook.Writer.Err(line)
	case log.WarnLevel:
		return hook.Writer.Warning(line)
	case log.InfoLevel:
		return hook.Writer.Info(line)
	case log.DebugLevel:
		return hook.Writer.Debug(line)
	default:
		return nil
	}
}

func (hook *SyslogHook) Levels() []log.Level {
	return []log.Level{
		log.PanicLevel,
		log.FatalLevel,
		log.ErrorLevel,
		log.WarnLevel,
		log.InfoLevel,
		log.DebugLevel,
	}
}
