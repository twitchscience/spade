package writer

import (
	"os"
	"syscall"
	"time"
)

func isRotateNeeded(inode os.FileInfo, conditions RotateConditions) (bool, time.Time) {
	stats := inode.Sys().(*syscall.Stat_t)
	createdAt := time.Unix(stats.Atim.Sec, stats.Atim.Nsec)
	return inode.Size() > conditions.MaxLogSize || time.Now().Sub(createdAt) > conditions.MaxTimeAllowed, createdAt
}
