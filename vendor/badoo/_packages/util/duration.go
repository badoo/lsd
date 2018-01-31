package util

import (
	"time"
	"syscall"
)

func Float64ToDuration(seconds float64) time.Duration {
	return time.Duration(seconds * float64(time.Second))
}

func Float32ToDuration(seconds float32) time.Duration {
	return time.Duration(seconds * float32(time.Second))
}

func TimevalToFloat32(tv *syscall.Timeval) float32 {
	return float32(tv.Sec) + float32(tv.Usec)/float32(1000*1000)
}
