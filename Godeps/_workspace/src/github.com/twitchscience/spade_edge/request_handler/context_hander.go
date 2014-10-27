package request_handler

import (
	"strconv"
	"strings"
	"time"

	"github.com/cactus/go-statsd-client/statsd"
)

// TODO naming?
type timerInstance struct {
	start time.Time
}

func newTimerInstance() *timerInstance {
	return &timerInstance{
		start: time.Now(),
	}
}

func (t *timerInstance) stopTiming() (r time.Duration) {
	r = time.Now().Sub(t.start)
	t.start = time.Now()
	return
}

type requestContext struct {
	Now      time.Time
	Method   string
	IpHeader string
	Endpoint string
	Timers   map[string]time.Duration
	Status   int
}

func (r *requestContext) setStatus(s int) *requestContext {
	r.Status = s
	return r
}

func (r *requestContext) recordStats(statter statsd.Statter) {
	prefix := strings.Join([]string{
		r.Method,
		strings.Replace(r.Endpoint, ".", "_", -1),
		strconv.Itoa(r.Status),
	}, ".")
	for stat, duration := range r.Timers {
		statter.Timing(prefix+"."+stat, duration.Nanoseconds(), 0.1)
	}
}
