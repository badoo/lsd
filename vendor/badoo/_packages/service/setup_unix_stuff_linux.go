// +build linux

package service

import (
	"badoo/_packages/log"

	"fmt"
	"io/ioutil"
	"strconv"
	"strings"
)

func setupUnixStuff() error {
	// check global machine somaxconn
	const (
		somaxconn_file = "/proc/sys/net/core/somaxconn"
		somaxconn_needed_value = 65535
	)

	data, err := ioutil.ReadFile(somaxconn_file)
	if err != nil {
		return fmt.Errorf("ioutil.ReadFile(%s) failed", somaxconn_file)
	}

	somaxconn_value_string := strings.TrimSpace(string(data))

	somaxconn_value, err := strconv.Atoi(somaxconn_value_string);
	if err != nil {
		return fmt.Errorf("strconv.Atoi(%q) failed, %v", somaxconn_value_string, err)
	}

	if (somaxconn_value < somaxconn_needed_value) {
		log.Errorf("change somaxconn %d -> %d manually", somaxconn_value, somaxconn_needed_value);
	}

	return nil
}
