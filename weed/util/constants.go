package util

import (
	"fmt"
)

var (
	VERSION_NUMBER = fmt.Sprintf("%.02f", 3.28)
	VERSION        = sizeLimit + " " + VERSION_NUMBER
	COMMIT         = ""

	VERSIONMore = sizeLimit + " " + VERSION_NUMBER + " " + BuiltCommit + " quota, built: " + BuiltTime
	BuiltTime   string
	BuiltCommit string
)

func Version() string {
	return VERSIONMore + " " + COMMIT
}
