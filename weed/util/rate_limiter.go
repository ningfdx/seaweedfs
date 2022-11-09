package util

import (
	"golang.org/x/time/rate"
	"time"
)

const MBpsLimit = 1024 * 1024

func NewLimiter(MBps int) *rate.Limiter {
	burst := 10 * MBpsLimit
	l := rate.NewLimiter(rate.Limit(MBps), burst)
	// spend initial burst
	l.AllowN(time.Now(), burst)
	return l
}
