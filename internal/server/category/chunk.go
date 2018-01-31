package category

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

const DATE_FORMAT = "2006-01-02"

func parseFileName(categoryName, fileName string) (string, uint64, error) {

	prefix := categoryName + "-"
	if !strings.HasPrefix(fileName, prefix) {
		return "", 0, fmt.Errorf("filename does not start with %s", prefix)
	}
	fileName = strings.TrimPrefix(fileName, prefix)
	parts := strings.Split(fileName, "_")
	if len(parts) == 1 {
		return "", 0, errors.New("no counter in filename")
	}
	if len(parts) > 2 {
		return "", 0, fmt.Errorf("too many underscores in filename")
	}
	counterParts := strings.SplitN(parts[1], ".", 2) // ignore possible extensions
	counter, err := strconv.ParseUint(counterParts[0], 10, 64)
	if err != nil {
		return "", 0, fmt.Errorf("failed to parse counter from %s", parts[1])
	}
	return parts[0], counter, nil
}

type chunkInfo struct {
	category string
	date     string
	counter  uint64
}

func (c *chunkInfo) isNewerThan(otherCi *chunkInfo) bool {
	if c.date > otherCi.date {
		return true
	}
	if c.date < otherCi.date {
		return false
	}
	return c.counter > otherCi.counter
}

func (c *chunkInfo) increment() {
	oldDate := c.date
	c.date = time.Now().Format(DATE_FORMAT)
	if oldDate != c.date {
		c.counter = 0
	}
	c.counter++
}

func (c *chunkInfo) makeFilename(extension string) string {
	return fmt.Sprintf("%s-%s_%06d%s", c.category, c.date, c.counter, extension)
}
