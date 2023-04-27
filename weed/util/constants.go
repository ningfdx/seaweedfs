package util

import (
	"fmt"
)

var (
	VERSION_NUMBER = fmt.Sprintf("%.02f", 3.47)
	VERSION        = sizeLimit + " " + VERSION_NUMBER
	COMMIT         = ""

	VERSIONMore = sizeLimit + " " + VERSION_NUMBER + " " + BuiltCommit + " quota, built: " + BuiltTime + ", " + BuiltType
	BuiltTime   string
	BuiltCommit string

	// BuiltType '' = server, 'client' = client
	BuiltType string
)

func Version() string {
	return VERSIONMore + " " + COMMIT
}
