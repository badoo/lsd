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
	RESTART_ENV_VAR        = "GO_SERVICE_RESTART"
	RESTART_PIDFILE_SUFFIX = ".tmp"
)

var (
	currentRestart *RestartContext
)

type RestartSocket struct {
	Address string
	Fd      uintptr
}

type RestartSockets map[string]RestartSocket

type RestartChildData struct {
	PPid            int
	GpbrpcSockets   RestartSockets
	HttpPProfSocket RestartSocket
	// private, not serialized
	files []*os.File
}

type RestartProcStatus struct {
	State *os.ProcessState
	Err   error
}

type RestartContext struct {
	Child         *os.Process
	ChildWaitC    chan RestartProcStatus
	ChildTimeoutC <-chan time.Time
	Pidfile       *Pidfile
}

func (rctx *RestartContext) MovePidfileBack() (err error) {
	pidfile, err = currentRestart.Pidfile.MoveTo(pidfile.path)
	if err != nil {
		log.Errorf("can't move pidfile back: %v, %v", err, currentRestart.Pidfile)
	}

	return err
}

// ----------------------------------------------------------------------------------------------------------------------------------

func RestartInprogress() bool {
	return currentRestart != nil
}

func RestartChildWaitChan() chan RestartProcStatus {
	if currentRestart == nil {
		return nil
	}

	return currentRestart.ChildWaitC
}

func RestartChildTimeoutChan() <-chan time.Time {
	if currentRestart == nil {
		return nil
	}

	return currentRestart.ChildTimeoutC
}

func ParseRestartDataFromEnv() (*RestartChildData, error) {
	rcd := &RestartChildData{}
	rcd_env := os.Getenv(RESTART_ENV_VAR)

	if rcd_env == "" {
		return nil, nil // ok, but no data is set
	}

	log.Debugf("ParseRestartDataFromEnv; %s: %s", RESTART_ENV_VAR, rcd_env)
	if err := json.Unmarshal([]byte(rcd_env), rcd); err != nil {
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
func InitiateRestart() error {

	if RestartInprogress() {
		return fmt.Errorf("restart already inprogress")
	}

	// XXX: tried to move gathering childData into it's own function, hard to get closing all files right with just defer :(
	childData := &RestartChildData{
		PPid:          os.Getpid(),
		GpbrpcSockets: make(RestartSockets),
		files:         []*os.File{},
	}
	defer func() { // close dup()-d files on exit (needs to be before we start populating files list, in case of any errors)
		for _, file := range childData.files {
			file.Close()
		}
	}()

	addFd := func() func(addr string) RestartSocket {
		fdOffset := 3
		return func(addr string) RestartSocket {
			rs := RestartSocket{
				Address: addr,
				Fd:      uintptr(fdOffset),
			}
			fdOffset++
			return rs
		}
	}()

	if HttpServer != nil {
		dupFile, err := dupFdFromListener(HttpServer.Listener)
		if err != nil {
			return fmt.Errorf("can't export fd for http_pprof_addr, err: %v", err)
		}
		childData.files = append(childData.files, dupFile)
		childData.HttpPProfSocket = addFd(HttpServer.Addr)
	}

	for _, server := range StartedServers {
		dupFile, err := dupFdFromListener(server.Server.Listener)
		if err != nil {
			return fmt.Errorf("can't export fd for %s, err: %v", server.Name, err)
		}

		childData.files = append(childData.files, dupFile)
		childData.GpbrpcSockets[server.Name] = addFd(server.Address)
	}

	var tmpPidfile *Pidfile
	var err error

	// move parent's pidfile somewhere child won't start otherwise)
	if pidfile != nil && pidfile.path != "" {
		tmpPidfile, err = pidfile.MoveTo(pidfile.path + RESTART_PIDFILE_SUFFIX)
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

func FinalizeRestartWithSuccess() {
	log.Debug("restarted successfuly")
	currentRestart.Pidfile.CloseAndRemove()
	currentRestart = nil
}

func FinalizeRestartWithError(proc_status RestartProcStatus) {

	if proc_status.Err != nil {
		log.Errorf("couldn't collect state for child %d, %v", currentRestart.Child.Pid, proc_status.Err)
	}
	log.Warnf("child %d failed to start, collected %v", currentRestart.Child.Pid, proc_status.State)

	// not waiting for child, so have to release
	currentRestart.Child.Release()

	currentRestart.MovePidfileBack()
	currentRestart = nil
}

func FinalizeRestartWithTimeout() {

	currentRestart.Child.Kill()
	child_state, err := currentRestart.Child.Wait()
	log.Warnf("child %d failed to start and was killed, state: %v, err: %v", currentRestart.Child.Pid, child_state, err)

	currentRestart.MovePidfileBack()
	currentRestart = nil
}

// ----------------------------------------------------------------------------------------------------------------------------------
// helpers

func restartRunChild(childData *RestartChildData) (*RestartContext, error) {
	if currentRestart != nil {
		panic("can't call this function when restart is already in progress")
	}

	child_data_json, err := json.Marshal(childData)
	if err != nil {
		return nil, fmt.Errorf("can't json encode child data: %v", err)
	}

	os.Setenv(RESTART_ENV_VAR, string(child_data_json))
	log.Debugf("%s = %s", RESTART_ENV_VAR, child_data_json)

	// can start child now
	child, err := forkExec(childData.files)
	if err != nil {
		return nil, fmt.Errorf("forkExec error: %v", err)
	}

	log.Debugf("started child: %d", child.Pid)

	os.Setenv(RESTART_ENV_VAR, "") // reset env after child starts, just in case

	// save state
	rctx := &RestartContext{
		Child:         child,
		ChildWaitC:    make(chan RestartProcStatus, 1), // needs to be buffered (in case we drop restart state before goroutine has the chance to write)
		ChildTimeoutC: time.After(5 * time.Second),     // FIXME: make this configureable?
	}

	// start child wait goroutine, this goroutine never dies if child starts up successfuly
	//   but it doesn't matter, since this process will exit soon in that case
	go func(rctx *RestartContext) {
		status, err := rctx.Child.Wait()
		rctx.ChildWaitC <- RestartProcStatus{status, err}
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

func forkExec(save_files []*os.File) (*os.Process, error) {
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
	files = append(files, save_files...)

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
