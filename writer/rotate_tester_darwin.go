package writer

import (
	"os"
	"syscall"
	"time"
)

func isRotateNeeded(inode os.FileInfo, name string) (bool, time.Time) {
	stats := inode.Sys().(*syscall.Stat_t)
	createdAt := time.Unix(stats.Atimespec.Sec, stats.Atimespec.Nsec)
	return inode.Size() > MaxLogSize || (time.Now().Sub(createdAt)) > MaxTimeAllowed, createdAt
}
