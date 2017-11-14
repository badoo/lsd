package util

import (
	"crypto/rand"
	"fmt"
)

// UUID() generates v4 UUID and returns it as string
// implemenation from https://groups.google.com/forum/#!topic/golang-nuts/Rn13T6BZpgE
func UUID() string {
	b := make([]byte, 16)
	rand.Read(b) // ignore error

	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}
