package util

import (
	"fmt"
)

var (
	VERSION_NUMBER = fmt.Sprintf("%.02f", 3.20)
	VERSION        = sizeLimit + " " + VERSION_NUMBER
	COMMIT         = ""

	VERSIONMore = sizeLimit + " " + VERSION_NUMBER + " quota, built: " + BuiltTime
	BuiltTime   string
)

func Version() string {
	return VERSIONMore + " " + COMMIT
}
