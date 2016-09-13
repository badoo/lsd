package service

import (
	"badoo/_packages/log"
	syslog_hook "badoo/_packages/log/hooks/syslog"
	"fmt"
	"log/syslog"
	"os"
	"regexp"
)

var (
	logPath          = "-"
	logLevel         = log.DebugLevel
	syslogHookInited = false

	stderrLogger *log.Logger // for when you need a console unconditionally (such as config test errors)
)

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

func init() {
	log.SetLevel(log.DebugLevel)
	log.SetFormatter(&log.BadooFormatter{})

	stderrLogger = &log.Logger{
		Out:       os.Stderr,
		Formatter: &log.BadooFormatter{},
		Level:     log.DebugLevel,
	}

}

func reopenLogfile(path string, level log.Level) (err error) {
	if err := initSyslogHook(); err != nil {
		return err
	}

	log.SetLevel(level)

	if "" == path || "-" == path {
		log.SetOutput(os.Stderr)
	} else {
		logfile, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
		if nil != err {
			return err
		}
		log.SetOutput(logfile)
	}

	logPath = path
	logLevel = level

	return nil
}

func GetLogLevel() log.Level {
	return logLevel
}
