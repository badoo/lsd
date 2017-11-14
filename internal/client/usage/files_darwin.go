// build -linux

package usage

import (
	"badoo/_packages/log"
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
)

// get unused files with inodes specified in filter map
func getFreeFiles(filter map[uint64]bool) (map[uint64]bool, error) {

	result := make(map[uint64]bool)
	for ino := range filter {
		result[ino] = true
	}

	cmd := exec.Command("lsof", "-F", "-p", fmt.Sprintf("^%d", os.Getpid()))
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}

	var b bytes.Buffer
	cmd.Stderr = &b

	if err = cmd.Start(); err != nil {
		log.Printf("Stderr from lsof: %s", b.String())
		return nil, err
	}

	i := 0
	r := bufio.NewReader(stdout)
	for {
		ln, err := r.ReadString('\n')
		i++
		if err != nil {
			if err == io.EOF {
				break
			} else {
				log.Errorf("Could not read response from lsof: %s", err.Error())
				log.Printf("Stderr from lsof: %s", b.String())
				waitErr := cmd.Wait()
				if waitErr != nil {
					log.Errorf("Wait for lsof process failed: %v", waitErr)
				}
				return nil, err
			}
		}

		if ln[0] != 'i' || len(ln) <= 1 {
			continue
		}

		ino, err := strconv.ParseUint(ln[1:len(ln)-1], 10, 64)
		if err != nil {
			log.Printf("Could not parse inode number '%s': %s", ln, err.Error())
			continue
		}

		delete(result, ino)
	}

	err = cmd.Wait()
	if err != nil {
		log.Printf("Stderr from lsof: %s", b.String())
		return nil, err
	}

	return result, nil
}
