package service

import (
	"badoo/_packages/log"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"syscall"
	"time"
)

const (
	restartEnvVar        = "GO_SERVICE_RESTART"
	restartPidfileSuffix = ".tmp"
)

var (
	currentRestart *restartContext
)

type restartSocket struct {
	Address string
	Fd      uintptr
}

type restartSockets map[string]restartSocket

type restartChildData struct {
	PPid            int
	GpbrpcSockets   restartSockets
	HTTPPProfSocket restartSocket
	// private, not serialized
	files []*os.File
}

type restartProcStatus struct {
	State *os.ProcessState
	Err   error
}

type restartContext struct {
	Child         *os.Process
	ChildWaitC    chan restartProcStatus
	ChildTimeoutC <-chan time.Time
	Pidfile       *PidfileCtx
}

func (rctx *restartContext) MovePidfileBack() (err error) {
	if currentRestart.Pidfile != nil { // will be == nil, when pidfile is not set in config
		pidfile, err = currentRestart.Pidfile.MoveTo(pidfile.path)
		if err != nil {
			log.Errorf("can't move pidfile back: %v, %v", err, currentRestart.Pidfile)
		}
	}

	return err
}

// ----------------------------------------------------------------------------------------------------------------------------------

func restartInprogress() bool {
	return currentRestart != nil
}

func restartChildWaitChan() chan restartProcStatus {
	if currentRestart == nil {
		return nil
	}

	return currentRestart.ChildWaitC
}

func restartChildTimeoutChan() <-chan time.Time {
	if currentRestart == nil {
		return nil
	}

	return currentRestart.ChildTimeoutC
}

func parseRestartDataFromEnv() (*restartChildData, error) {
	rcd := &restartChildData{}
	rcdEnv := os.Getenv(restartEnvVar)

	if rcdEnv == "" {
		return nil, nil // ok, but no data is set
	}

	log.Debugf("ParseRestartDataFromEnv; %s: %s", restartEnvVar, rcdEnv)
	if err := json.Unmarshal([]byte(rcdEnv), rcd); err != nil {
		return nil, err
	}

	if rcd.PPid <= 0 {
		return nil, fmt.Errorf("restart_data.Ppid <= 0 (%d)", rcd.PPid)
	}

	if rcd.GpbrpcSockets == nil {
		return nil, fmt.Errorf("restart_data.GpbrpcSockets == nil")
	}

	return rcd, nil
}

func dupFdFromListener(l net.Listener) (*os.File, error) {
	switch listener := l.(type) {
	case *net.TCPListener:
		return listener.File()
	case *net.UnixListener:
		return listener.File()
	default:
		return nil, fmt.Errorf("restart unsupported listener %T", listener)
	}
}

// initiate graceful restart process
//  *CAN NOT BE CALLED concurrently* as 'restart in progress' flag is not set immediately
func initiateRestart() error {

	if restartInprogress() {
		return fmt.Errorf("restart already inprogress")
	}

	// XXX: tried to move gathering childData into it's own function, hard to get closing all files right with just defer :(
	childData := &restartChildData{
		PPid:          os.Getpid(),
		GpbrpcSockets: make(restartSockets),
		files:         []*os.File{},
	}
	defer func() { // close dup()-d files on exit (needs to be before we start populating files list, in case of any errors)
		for _, file := range childData.files {
			file.Close()
		}
	}()

	addFd := func() func(addr string) restartSocket {
		fdOffset := 3
		return func(addr string) restartSocket {
			rs := restartSocket{
				Address: addr,
				Fd:      uintptr(fdOffset),
			}
			fdOffset++
			return rs
		}
	}()

	if httpServer != nil {
		dupFile, err := dupFdFromListener(httpServer.Listener)
		if err != nil {
			return fmt.Errorf("can't export fd for http_pprof_addr, err: %v", err)
		}
		childData.files = append(childData.files, dupFile)
		childData.HTTPPProfSocket = addFd(httpServer.Addr)
	}

	for _, server := range startedServers {
		dupFile, err := dupFdFromListener(server.server.Listener())
		if err != nil {
			return fmt.Errorf("can't export fd for %s, err: %v", server.name, err)
		}

		childData.files = append(childData.files, dupFile)
		childData.GpbrpcSockets[server.name] = addFd(server.confAddress)
	}

	var tmpPidfile *PidfileCtx
	var err error

	// move parent's pidfile somewhere child won't start otherwise)
	if pidfile != nil && pidfile.path != "" {
		tmpPidfile, err = pidfile.MoveTo(pidfile.path + restartPidfileSuffix)
		if err != nil {
			return fmt.Errorf("can't move pidfile: %v", err)
		}

		// will need to move the pidfile back in case of any further errors
		defer func() {
			if err != nil && tmpPidfile != nil {
				var e1 error // want to reuse global pidfile below, not redefine it (and preserve original err to return it)

				pidfile, e1 = tmpPidfile.MoveTo(pidfile.path)
				if e1 != nil {
					log.Errorf("[you'll now work without pidfile] can't move pidfile back: %v", e1)
				}
			}
		}()
	}

	currentRestart, err = restartRunChild(childData)
	if err != nil {
		return err
	}

	currentRestart.Pidfile = tmpPidfile

	return nil
}

func finalizeRestartWithSuccess() {
	log.Debug("restarted successfuly")

	if currentRestart.Pidfile != nil {
		currentRestart.Pidfile.CloseAndRemove()
	}
	currentRestart = nil
}

func finalizeRestartWithError(procStatus restartProcStatus) {

	if procStatus.Err != nil {
		log.Errorf("couldn't collect state for child %d, %v", currentRestart.Child.Pid, procStatus.Err)
	}
	log.Warnf("child %d failed to start, collected %v", currentRestart.Child.Pid, procStatus.State)

	// not waiting for child, so have to release
	currentRestart.Child.Release()

	currentRestart.MovePidfileBack()
	currentRestart = nil
}

func finalizeRestartWithTimeout() {

	currentRestart.Child.Kill()
	childState, err := currentRestart.Child.Wait()
	log.Warnf("child %d failed to start and was killed, state: %v, err: %v", currentRestart.Child.Pid, childState, err)

	currentRestart.MovePidfileBack()
	currentRestart = nil
}

// ----------------------------------------------------------------------------------------------------------------------------------
// helpers

func restartRunChild(childData *restartChildData) (*restartContext, error) {
	if currentRestart != nil {
		panic("can't call this function when restart is already in progress")
	}

	childDataJSON, err := json.Marshal(childData)
	if err != nil {
		return nil, fmt.Errorf("can't json encode child data: %v", err)
	}

	os.Setenv(restartEnvVar, string(childDataJSON))
	log.Debugf("%s = %s", restartEnvVar, childDataJSON)

	// can start child now
	child, err := forkExec(childData.files)
	if err != nil {
		return nil, fmt.Errorf("forkExec error: %v", err)
	}

	log.Debugf("started child: %d", child.Pid)

	os.Setenv(restartEnvVar, "") // reset env after child starts, just in case

	// save state
	rctx := &restartContext{
		Child:         child,
		ChildWaitC:    make(chan restartProcStatus, 1), // needs to be buffered (in case we drop restart state before goroutine has the chance to write)
		ChildTimeoutC: time.After(5 * time.Second),     // FIXME: make this configureable?
	}

	// start child wait goroutine, this goroutine never dies if child starts up successfuly
	//   but it doesn't matter, since this process will exit soon in that case
	go func(rctx *restartContext) {
		status, err := rctx.Child.Wait()
		rctx.ChildWaitC <- restartProcStatus{status, err}
	}(rctx)

	return rctx, nil
}

func lookPath() (argv0 string, err error) {
	argv0, err = exec.LookPath(os.Args[0])
	if nil != err {
		return
	}
	if _, err = os.Stat(argv0); nil != err {
		return
	}
	return
}

func forkExec(saveFiles []*os.File) (*os.Process, error) {
	binary, err := lookPath()
	if err != nil {
		return nil, err
	}

	wd, err := os.Getwd()
	if err != nil {
		return nil, err
	}

	files := []*os.File{
		os.Stdin,
		os.Stdout,
		os.Stderr,
	}
	files = append(files, saveFiles...)

	attr := &os.ProcAttr{
		Dir:   wd,
		Env:   os.Environ(),
		Files: files,
		Sys:   &syscall.SysProcAttr{},
	}
	child, err := os.StartProcess(binary, os.Args, attr)
	if err != nil {
		return nil, err
	}

	return child, nil
}
