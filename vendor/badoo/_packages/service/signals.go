package service

import (
	"badoo/_packages/log"
	"fmt"
	"os"
	"os/signal"
	"syscall"
)

// waitForSignals tells the caller how to exit
type exitMethod int

const (
	exitImmediately = iota
	exitGracefully
)

func signalName(sig os.Signal) string {
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

func sigactionImmediateShutdown(sig os.Signal) {
	log.Infof("got %s, exiting NOW", signalName(sig))
}

func sigactionGracefulShutdown(sig os.Signal) {
	log.Infof("got %s, shutting down gracefully", signalName(sig))
}

func sigactionReopenLogs(sig os.Signal) {
	log.Infof("got %s, reopening logfile: %s", signalName(sig), logPath)

	if err := reopenLogfile(logPath, logLevel); err != nil {
		log.Errorf("can't reopen log file: %s", err)
	}

	log.Infof("sigaction__reopen_logs: new log opened: %s", logPath)
}

func sigactionGracefulRestart(sig os.Signal) {
	log.Infof("got %s, restarting gracefully", signalName(sig))

	if err := initiateRestart(); err != nil {
		log.Errorf("can't initiate restart: %s", err)
	}
}

func waitForSignals() exitMethod {
	c := make(chan os.Signal, 5)
	signal.Notify(c, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGUSR1, syscall.SIGINT, syscall.SIGUSR2)

	for {
		select {
		case sig := <-c:

			switch sig {
			case syscall.SIGINT:
				sigactionImmediateShutdown(sig)
				return exitImmediately

			case syscall.SIGTERM:
				sigactionGracefulShutdown(sig)
				return exitGracefully

			case syscall.SIGUSR1:
				if restartInprogress() == false { // FIXME: why ingore log reopen ?
					sigactionReopenLogs(sig)
				} else {
					log.Infof("service is restarting. ignoring %s", signalName(sig))
				}

			case syscall.SIGUSR2:
				if restartInprogress() == false {
					sigactionGracefulRestart(sig)
				} else {
					log.Infof("service is restarting. ignoring %s", signalName(sig))
				}

			case syscall.SIGQUIT:
				if restartInprogress() == false {
					// not a restart sequence, someone's just sent us SIGQUIT
					sigactionGracefulShutdown(sig)
					return exitGracefully
				}

				finalizeRestartWithSuccess()
				return exitGracefully
			}

		case wp := <-restartChildWaitChan():
			finalizeRestartWithError(wp)

		case <-restartChildTimeoutChan():
			finalizeRestartWithTimeout()
		}
	}
}
