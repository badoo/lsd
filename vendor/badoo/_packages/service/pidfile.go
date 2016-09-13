package service

import (
	"fmt"
	"os"
	"syscall"
)

type Pidfile struct {
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
func PidfileTest(path string) error {
	pf, err := pidfileOpenFile(path)
	pf.Close()
	return err
}

// PidfileOpen opens new pidfile, locks it, truncates and writes current pid to it
func PidfileOpen(path string) (*Pidfile, error) {

	pf, err := pidfileOpenFile(path)
	if err != nil {
		return nil, err
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
	return &Pidfile{
		f:    pf,
		path: path,
	}, nil
}

func (p *Pidfile) Close() (err error) {
	if p.f != nil {
		err = p.f.Close()
		p.f = nil
	}
	return
}

func (p *Pidfile) MoveTo(new_path string) (*Pidfile, error) {
	if p.f == nil {
		return nil, fmt.Errorf("no pidfile is open")
	}

	if new_path == "" {
		return nil, fmt.Errorf("need path for new pidfile")
	}

	if err := os.Rename(p.path, new_path); err != nil {
		return nil, err
	}

	new_p := &Pidfile{
		f:    p.f,
		path: new_path,
	}

	p.f = nil

	return new_p, nil
}

func (p *Pidfile) CloseAndRemove() error {
	if p.f != nil {
		p.Close() // screw error here
		return os.Remove(p.path)
	}
	return nil
}
