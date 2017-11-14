package service

import (
	"badoo/_packages/log"
	syslog_hook "badoo/_packages/log/hooks/syslog"
	"fmt"
	"io"
	"log/syslog"
	"os"
	"regexp"
)

var (
	logLevel log.Level // current log level
	logPath  string    // currently open logfile path
	logFile  *os.File  // currently open logfile (only if real file, that needs to be closed)

	syslogHookInited = false

	stderrLogger *log.Logger // for when you need a console unconditionally (such as config test errors)
)

func init() {
	logLevel = log.DebugLevel
	logPath = ""  // stderr
	logFile = nil // stderr is not a real file

	log.SetLevel(logLevel)
	log.SetFormatter(&log.BadooFormatter{})

	stderrLogger = &log.Logger{
		Out:       os.Stderr,
		Formatter: &log.BadooFormatter{},
		Level:     logLevel,
	}
}

func parseSyslogIdentity(str string) string {
	if str == "" {
		return ""
	}

	type replaceTable struct {
		regExp  string
		replace string
	}

	table := []replaceTable{
		{"\\$\\{daemon\\}", VersionInfo.GetVcsBasename()},
		{"\\$\\{version\\}", VersionInfo.GetVersion()},
		{"\\$\\{service_name\\}", DaemonConfig().GetServiceName()},
		{"\\$\\{service_instance_name\\}", DaemonConfig().GetServiceInstanceName()},
	}

	for _, elem := range table {
		re := regexp.MustCompile(elem.regExp)
		str = re.ReplaceAllString(str, elem.replace)
	}

	return str
}

func initSyslogHook() error {
	if syslogHookInited {
		return nil
	}

	if DaemonConfig().SyslogIdentity != nil {
		syslogIdentity := parseSyslogIdentity(DaemonConfig().GetSyslogIdentity())
		log.Debugf("syslog_identity: <%s>", syslogIdentity)

		syslogAddr := fmt.Sprintf("%s:%d", DaemonConfig().GetSyslogIp(), DaemonConfig().GetSyslogPort())

		network := func() string {

			if DaemonConfig().GetSyslogIp() != "" && DaemonConfig().GetSyslogPort() != 0 {
				return "udp"
			}

			return ""
		}()

		hook, err := syslog_hook.NewSyslogHookNoFrozen(network, syslogAddr, syslog.LOG_LOCAL6, syslogIdentity)
		if err != nil {
			return err
		}

		log.AddHook(hook)

		if DaemonConfig().GetSyslogSendBufferSize() != 0 {
			log.Warn("unsupported config option syslog_send_buffer_size, ignored")
		}
	}

	syslogHookInited = true

	return nil
}

func reopenLogfile(path string, level log.Level) (err error) {

	if err := initSyslogHook(); err != nil {
		return err
	}

	log.SetLevel(level)

	// where are we going to write shit to?
	newOutput, newFile, err := func() (io.Writer, *os.File, error) {
		if path == "" || path == "-" {
			return os.Stderr, nil, nil
		} else {
			file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
			return file, file, err
		}
	}()

	if err != nil {
		return err
	}

	// reopen for real, remember to close old one
	log.SetOutput(newOutput)
	if logFile != nil {
		logFile.Close()
	}

	logPath = path
	logLevel = level
	logFile = newFile

	return nil
}

func GetLogLevel() log.Level {
	return log.StandardLogger().Level
}
