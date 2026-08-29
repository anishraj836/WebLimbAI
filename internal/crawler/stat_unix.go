//go:build !windows

package crawler

import (
	"fmt"
	"io/fs"
	"syscall"
)

func getFileIdentifier(info fs.FileInfo) string {
	if stat, ok := info.Sys().(*syscall.Stat_t); ok {
		return fmt.Sprintf("%d:%d", stat.Dev, stat.Ino)
	}
	return ""
}
