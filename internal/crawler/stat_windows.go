//go:build windows

package crawler

import (
	"io/fs"
)

func getFileIdentifier(info fs.FileInfo) string {
	return ""
}
