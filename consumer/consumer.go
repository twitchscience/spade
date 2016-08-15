package consumer

import (
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/cactus/go-statsd-client/statsd"
	"github.com/twitchscience/aws_utils/logger"
	"github.com/twitchscience/kinsumer"
	kstatsd "github.com/twitchscience/kinsumer/statsd"
)

// Config is used to set configuration variables for the Consumer
type Config struct {
	// ApplicationName is the name that kinsumer uses to communicate with other clients
	// consuming the same kinesis stream
	ApplicationName string

	// StreamName is the name of the stream that is being consumed from
	StreamName string

	// (Optional) Time for Kinsumer to sleep if there are no new records
	ThrottleDelay string

	// (Optional) Delay before the checkpoint for each shard is commited to the database
	CommitFrequency string

	// (Optional) How frequently the list of shards are checked
	ShardCheckFrequency string

	// (Optional) Size of the internal buffer for kinesis events
	BufferSize int
}

// Result is the next data/error to be consumed from the kinsumer.
type Result struct {
	Data  []byte
	Error error
}

// Consumer is an object that consumes events from Kinesis
type Consumer struct {
	// C is used to read records off the kinsumer queue
	C <-chan *Result

	// send is a write only alias to C
	send chan<- *Result

	closer   chan struct{}
	kinsumer *kinsumer.Kinsumer
	sync.WaitGroup
}

func configEntryToDuration(entry string) (time.Duration, error) {
	if len(entry) == 0 {
		return 0, nil
	}
	d, e := time.ParseDuration(entry)
	if d < 0 {
		return 0, fmt.Errorf("%s is a negative duration", entry)
	}
	return d, e
}

func configToKinsumerConfig(config Config) (kinsumer.Config, error) {
	kinsumerConfig := kinsumer.NewConfig()
	d, e := configEntryToDuration(config.ThrottleDelay)
	if e != nil {
		return kinsumerConfig, fmt.Errorf("Invalid ThrottleDelay: %s", e)
	}
	if d > 0 {
		kinsumerConfig = kinsumerConfig.WithThrottleDelay(d)
	}

	d, e = configEntryToDuration(config.CommitFrequency)
	if e != nil {
		return kinsumerConfig, fmt.Errorf("Invalid CommitFrequency: %s", e)
	}
	if d > 0 {
		kinsumerConfig = kinsumerConfig.WithCommitFrequency(d)
	}

	d, e = configEntryToDuration(config.ShardCheckFrequency)
	if e != nil {
		return kinsumerConfig, fmt.Errorf("Invalid ShardCheckFrequency: %s", e)
	}
	if d > 0 {
		kinsumerConfig = kinsumerConfig.WithShardCheckFrequency(d)
	}

	if config.BufferSize < 0 {
		return kinsumerConfig, fmt.Errorf("Invalid (negative) BufferSize: %d", config.BufferSize)
	}
	if config.BufferSize > 0 {
		kinsumerConfig = kinsumerConfig.WithBufferSize(config.BufferSize)
	}

	return kinsumerConfig, nil
}

// New returns a newly created Consumer
func New(session *session.Session, stats statsd.Statter, config Config) (*Consumer, error) {
	kinsumerConfig, err := configToKinsumerConfig(config)
	if err != nil {
		return nil, err
	}

	kinsumerConfig = kinsumerConfig.WithStats(kstatsd.NewWithStatter(stats))
	hostname, _ := os.Hostname()
	kinsumer, err := kinsumer.NewWithSession(
		session,
		config.StreamName,
		config.ApplicationName,
		hostname,
		kinsumerConfig,
	)

	if err != nil {
		return nil, err
	}

	err = kinsumer.Run()
	if err != nil {
		return nil, err
	}

	channel := make(chan *Result)
	c := &Consumer{
		kinsumer: kinsumer,
		send:     channel,
		C:        channel,
		closer:   make(chan struct{}),
	}
	c.Add(1)
	logger.Go(func() {
		defer c.Done()
		c.crank()
	})
	return c, nil
}

func (c *Consumer) crank() {
	for {
		d, err := c.kinsumer.Next()
		select {
		case <-c.closer:
			return
		case c.send <- &Result{Data: d, Error: err}:
		}
	}
}

// Close closes down the kinesis consumption
func (c *Consumer) Close() {
	if c.kinsumer != nil {
		c.kinsumer.Stop()
		close(c.closer)
		c.Wait()
	}
}
