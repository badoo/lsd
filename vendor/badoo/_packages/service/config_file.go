package service

// XXX(antoxa): just leaving util wrappers here, to avoid changing services that depend on this code

import (
	"badoo/_packages/util"
)

func ParseConfigFromFile(conf_path string, to_struct interface{}) error {
	return util.ParseConfigFromFile(conf_path, to_struct)
}

func ParseConfigToStruct(data []byte, to_struct interface{}) error {
	return util.ParseConfigToStruct(data, to_struct)
}
