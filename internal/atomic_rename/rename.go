// +build !windows

package atomic_rename

import (
	"os"
)

func Rename(sourceFile, targetFile string) error {
	return os.Rename(sourceFile, targetFile)
}
