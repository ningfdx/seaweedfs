package util

import (
	"fmt"
)

var (
	VERSION_NUMBER = fmt.Sprintf("%.02f", 3.11)
	VERSION        = sizeLimit + " " + VERSION_NUMBER + " quota, built: " + BuiltTime

	COMMIT = ""

	BuiltTime string
)

func Version() string {
	return VERSION + " " + COMMIT
}
