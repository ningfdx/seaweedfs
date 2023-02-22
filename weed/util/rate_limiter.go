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

type MyContext struct {
	Cancel <-chan struct{}
}

func (ctx MyContext) Deadline() (deadline time.Time, ok bool) {
	return
}

func (ctx MyContext) Done() <-chan struct{} {
	return ctx.Cancel
}

func (ctx MyContext) Err() error {
	return nil
}

func (ctx MyContext) Value(key any) any {
	return nil
}
