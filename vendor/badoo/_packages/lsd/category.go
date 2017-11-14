package lsd

import (
	"fmt"
	"strings"
)

// Category is used to compute paths according to lsd format
type Category struct {
	BaseDir string
	Name    string
}

func (c *Category) GetDir() string {
	return fmt.Sprintf("%s/%s", strings.TrimRight(c.BaseDir, "/"), c.Name)
}

func (c *Category) GetCurrentSymlink() string {
	return fmt.Sprintf("%s/%s_current", c.GetDir(), c.Name)
}
