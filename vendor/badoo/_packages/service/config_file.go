package service

// XXX(antoxa): just leaving util wrappers here, to avoid changing services that depend on this code

import (
	"badoo/_packages/util"
)

func ParseConfigFromFile(confPath string, toStruct interface{}) error {
	return util.ParseConfigFromFile(confPath, toStruct)
}
