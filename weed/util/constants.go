package util

import (
	"fmt"
	"time"
)

var (
	VERSION_NUMBER = fmt.Sprintf("%.02f", 3.10)
	VERSION        = sizeLimit + " " + VERSION_NUMBER + " quota, built: " + time.Now().String()
	COMMIT         = ""
)

func Version() string {
	return VERSION + " " + COMMIT
}
