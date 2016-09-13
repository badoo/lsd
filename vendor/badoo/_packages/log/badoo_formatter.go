package log

import (
	"bytes"
	"fmt"
	"os"
	"sort"
)

type BadooFormatter struct {
	TextFormatter
}

func (f *BadooFormatter) Format(entry *Entry) ([]byte, error) {
	const layout = "Jan _2 15:04:05.000000"

	b := &bytes.Buffer{}
	fmt.Fprintf(b, "%s %s <%d> %s", entry.Time.Format(layout), getLevelStr(entry.Level), os.Getpid(), entry.Message)

	if len(entry.Data) > 0 {

		// FIXME(antoxa): disabling this sorting thing speeds this one up by ~35%
		var keys []string
		for k := range entry.Data {
			keys = append(keys, k)
		}
		sort.Strings(keys)

		b.WriteByte(' ')

		for _, key := range keys {
			f.appendKeyValue(b, key, entry.Data[key])
		}
	}

	b.WriteByte('\n')

	return b.Bytes(), nil
}

func getLevelStr(l Level) string {
	switch l {
	case DebugLevel:
		return "[DEBUG]"
	case InfoLevel:
		return "[INFO] "
	case WarnLevel:
		return "[WARN] "
	case ErrorLevel:
		return "[ERROR]"
	case FatalLevel:
		return "[FATAL]"
	case PanicLevel:
		return "[PANIC]"
	default:
		return "[UNKNOWN]"
	}
}
