package reporter

import (
	"time"

	"github.com/cactus/go-statsd-client/statsd"
)

type CactusStatterWrapper struct {
	statter statsd.Statter
	rate    float32
}

func WrapCactusStatter(statter statsd.Statter, rate float32) *CactusStatterWrapper {
	return &CactusStatterWrapper{
		statter: statter,
		rate:    rate,
	}
}

func (c *CactusStatterWrapper) Timing(stat string, t time.Duration) {
	c.statter.Timing(stat, int64(t), c.rate)
}

func (c *CactusStatterWrapper) IncrBy(stat string, value int) {
	c.statter.Inc(stat, int64(value), c.rate)
}
