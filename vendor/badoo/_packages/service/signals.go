package service

import (
	"badoo/_packages/log"
	"fmt"
	"os"
	"os/signal"
	"syscall"
)

// wait_for_signals tells the caller how to exit
type ExitMethod int

const (
	EXIT_IMMEDIATELY = iota
	EXIT_GRACEFULLY
)

func SignalName(sig os.Signal) string {
	switch sig {
	case syscall.SIGABRT:
		return "SIGABRT"
	case syscall.SIGALRM:
		return "SIGALRM"
	case syscall.SIGBUS:
		return "SIGBUS"
	case syscall.SIGCHLD:
		return "SIGCHLD"
	case syscall.SIGCONT:
		return "SIGCONT"
	case syscall.SIGFPE:
		return "SIGFPE"
	case syscall.SIGHUP:
		return "SIGHUP"
	case syscall.SIGILL:
		return "SIGILL"
	case syscall.SIGINT:
		return "SIGINT"
	case syscall.SIGIO:
		return "SIGIO"
	case syscall.SIGKILL:
		return "SIGKILL"
	case syscall.SIGPIPE:
		return "SIGPIPE"
	case syscall.SIGPROF:
		return "SIGPROF"
	case syscall.SIGQUIT:
		return "SIGQUIT"
	case syscall.SIGSEGV:
		return "SIGSEGV"
	case syscall.SIGSTOP:
		return "SIGSTOP"
	case syscall.SIGSYS:
		return "SIGSYS"
	case syscall.SIGTERM:
		return "SIGTERM"
	case syscall.SIGTRAP:
		return "SIGTRAP"
	case syscall.SIGTSTP:
		return "SIGTSTP"
	case syscall.SIGTTIN:
		return "SIGTTIN"
	case syscall.SIGTTOU:
		return "SIGTTOU"
	case syscall.SIGURG:
		return "SIGURG"
	case syscall.SIGUSR1:
		return "SIGUSR1"
	case syscall.SIGUSR2:
		return "SIGUSR2"
	case syscall.SIGVTALRM:
		return "SIGVTALRM"
	case syscall.SIGWINCH:
		return "SIGWINCH"
	case syscall.SIGXCPU:
		return "SIGXCPU"
	case syscall.SIGXFSZ:
		return "SIGXFSZ"
	default:
		return fmt.Sprintf("SIGNAL (%s)", sig)
	}
}

func sigaction__graceful_shutdown(sig os.Signal) {
	log.Infof("got %s, shutting down", SignalName(sig))
}

func sigaction__reopen_logs(sig os.Signal) {
	log.Infof("got %s, reopening logfile: %s", SignalName(sig), logPath)

	if err := reopenLogfile(logPath, logLevel); err != nil {
		log.Errorf("can't reopen log file: %s", err)
	}

	log.Infof("sigaction__reopen_logs: new log opened: %s", logPath)
}

func sigaction__graceful_restart(sig os.Signal) {
	log.Infof("got %s, restarting gracefully", SignalName(sig))

	if err := InitiateRestart(); err != nil {
		log.Errorf("can't initiate restart: %s", err)
	}
}

func wait_for_signals() ExitMethod {
	c := make(chan os.Signal, 5)
	signal.Notify(c, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGUSR1, syscall.SIGINT, syscall.SIGUSR2)

	for {
		select {
		case sig := <-c:

			switch sig {
			case syscall.SIGTERM, syscall.SIGINT:
				sigaction__graceful_shutdown(sig)
				return EXIT_IMMEDIATELY

			case syscall.SIGUSR1:
				if RestartInprogress() == false { // FIXME: why ingore log reopen ?
					sigaction__reopen_logs(sig)
				} else {
					log.Infof("service is restarting. ignoring %s", SignalName(sig))
				}

			case syscall.SIGUSR2:
				if RestartInprogress() == false {
					sigaction__graceful_restart(sig)
				} else {
					log.Infof("service is restarting. ignoring %s", SignalName(sig))
				}

			case syscall.SIGQUIT:
				if RestartInprogress() == false {
					// not a restart sequence, someone's just sent us SIGQUIT
					sigaction__graceful_shutdown(sig)
					return EXIT_IMMEDIATELY
				}

				FinalizeRestartWithSuccess()
				return EXIT_GRACEFULLY
			}

		case wp := <-RestartChildWaitChan():
			FinalizeRestartWithError(wp)

		case <-RestartChildTimeoutChan():
			FinalizeRestartWithTimeout()
		}
	}
}
