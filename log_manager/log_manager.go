package log_manager

import (
	"fmt"
	"os"
)

type Task interface {
	Filename() string
}

type LogTask struct {
	LogFile   os.FileInfo
	Directory string
}

func (l *LogTask) Filename() string {
	return fmt.Sprintf("%s/%s", l.Directory, l.LogFile.Name())
}
