package util

import (
	"fmt"
)

var (
	VERSION_NUMBER = fmt.Sprintf("%.02f", 3.10)
	VERSION        = sizeLimit + " " + VERSION_NUMBER + " nb "
	COMMIT         = ""
)

func Version() string {
	return VERSION + " " + COMMIT
}
