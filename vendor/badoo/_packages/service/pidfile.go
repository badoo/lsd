package service

import (
	"fmt"
	"os"
	"syscall"
)

type PidfileCtx struct {
	f    *os.File
	path string
}

func pidfileOpenFile(path string) (*os.File, error) {
	// just don't create pidfile in case path is not set
	if "" == path {
		return nil, nil
	}

	return os.OpenFile(path, os.O_WRONLY|os.O_CREATE, 0644)
}

// PidfileTest just opens the file and closes it again, used by testconf mode only
func pidfileTest(path string) error {
	pf, err := pidfileOpenFile(path)
	pf.Close()
	return err
}

// PidfileOpen opens new pidfile, locks it, truncates and writes current pid to it
func PidfileOpen(path string) (*PidfileCtx, error) {

	pf, err := pidfileOpenFile(path)
	if err != nil {
		return nil, err
	}

	if pf == nil { // not opened
		return nil, nil
	}

	defer func() {
		if err != nil {
			pf.Close()
		}
	}()

	err = syscall.Flock(int(pf.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if err != nil {
		return nil, fmt.Errorf("flock failed (other process running?): %s, %v", path, err)
	}

	err = pf.Truncate(0)
	if err != nil {
		return nil, err
	}

	_, err = fmt.Fprintf(pf, "%d\n", os.Getpid())
	if err != nil {
		return nil, err
	}

	// everything is ok
	return &PidfileCtx{
		f:    pf,
		path: path,
	}, nil
}

func (p *PidfileCtx) Close() (err error) {
	if p.f != nil {
		err = p.f.Close()
		p.f = nil
	}
	return
}

func (p *PidfileCtx) MoveTo(newPath string) (*PidfileCtx, error) {
	if p.f == nil {
		return nil, fmt.Errorf("no pidfile is open")
	}

	if newPath == "" {
		return nil, fmt.Errorf("need path for new pidfile")
	}

	if err := os.Rename(p.path, newPath); err != nil {
		return nil, err
	}

	newP := &PidfileCtx{
		f:    p.f,
		path: newPath,
	}

	p.f = nil

	return newP, nil
}

func (p *PidfileCtx) CloseAndRemove() error {
	if p.f != nil {
		p.Close() // screw error here
		return os.Remove(p.path)
	}
	return nil
}
