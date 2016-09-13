// os utilities
// getCurrentBinary code - ninja'd from https://github.com/kardianos/osext
package osutil

import (
	"fmt"
	"os"
	"runtime"
	"strings"
)

var (
	currentBinary string // cached
)

func GetCurrentBinary() (string, error) {
	var err error

	if currentBinary != "" {
		return currentBinary, nil
	}

	currentBinary, err = getCurrentBinary()
	return currentBinary, err
}

func getCurrentBinary() (string, error) {
	switch runtime.GOOS {
	case "linux":
		const deletedTag = " (deleted)"
		execpath, err := os.Readlink("/proc/self/exe")
		if err != nil {
			return execpath, err
		}
		execpath = strings.TrimSuffix(execpath, deletedTag)
		execpath = strings.TrimPrefix(execpath, deletedTag)
		return execpath, nil
	case "netbsd":
		return os.Readlink("/proc/curproc/exe")
	case "dragonfly":
		return os.Readlink("/proc/curproc/file")
	case "solaris":
		return os.Readlink(fmt.Sprintf("/proc/%d/path/a.out", os.Getpid()))
	case "darwin":
		return "", fmt.Errorf("no /proc on darwin")
	}
	return "", fmt.Errorf("osutil.GetCurrentBinary not implemented for %s", runtime.GOOS)
}
