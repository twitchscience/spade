package writer

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/cactus/go-statsd-client/statsd"
	"github.com/twinj/uuid"
	"github.com/twitchscience/aws_utils/logger"
	"github.com/twitchscience/spade/batcher"
	"github.com/twitchscience/spade/globber"
)

// KinesisWriterConfig is used to configure a KinesisWriter
// and it's nested globber and batcher objects
type KinesisWriterConfig struct {
	StreamName           string
	BufferSize           int
	MaxAttemptsPerRecord int
	RetryDelay           string

	Events map[string]struct {
		Filter string
		Fields []string
	}

	Globber globber.Config
	Batcher batcher.Config
}

// Validate returns an error if the config is not valid, or nil if it is
func (c *KinesisWriterConfig) Validate() error {
	err := c.Globber.Validate()
	if err != nil {
		return fmt.Errorf("globber config invalid: %s", err)
	}

	err = c.Batcher.Validate()
	if err != nil {
		return fmt.Errorf("batcher config invalid: %s", err)
	}

	_, err = time.ParseDuration(c.RetryDelay)
	if err != nil {
		return err
	}

	return nil
}

// KinesisWriter is a writer that writes events to kinesis
type KinesisWriter struct {
	incoming  chan *WriteRequest
	batches   chan [][]byte
	globber   *globber.Globber
	batcher   *batcher.Batcher
	config    KinesisWriterConfig
	client    *kinesis.Kinesis
	statter   statsd.Statter
	statNames map[int]string

	sync.WaitGroup
}

const (
	statPutRecordsAttempted = iota
	statPutRecordsLength
	statPutRecordsErrors
	statRecordsFailedThrottled
	statRecordsFailedInternalError
	statRecordsFailedUnknown
	statRecordsSucceeded
	statRecordsDropped
)

func generateStatNames(streamName string) map[int]string {
	stats := make(map[int]string)
	stats[statPutRecordsAttempted] = "kinesiswriter." + streamName + ".putrecords.attempted"
	stats[statPutRecordsLength] = "kinesiswriter." + streamName + ".putrecords.length"
	stats[statPutRecordsErrors] = "kinesiswriter." + streamName + ".putrecords.errors"
	stats[statRecordsFailedThrottled] = "kinesiswriter." + streamName + ".records_failed.throttled"
	stats[statRecordsFailedInternalError] = "kinesiswriter." + streamName + ".records_failed.internal_error"
	stats[statRecordsFailedUnknown] = "kinesiswriter." + streamName + ".records_failed.unknown_reason"
	stats[statRecordsSucceeded] = "kinesiswriter." + streamName + ".records_succeeded"
	stats[statRecordsDropped] = "kinesiswriter." + streamName + ".records_dropped"

	return stats
}

// NewKinesisWriter returns an instance of SpadeWriter that writes
// events to kinesis
func NewKinesisWriter(client *kinesis.Kinesis, statter statsd.Statter, config KinesisWriterConfig) (SpadeWriter, error) {
	err := config.Validate()
	if err != nil {
		return nil, err
	}
	w := &KinesisWriter{
		incoming:  make(chan *WriteRequest, config.BufferSize),
		batches:   make(chan [][]byte),
		config:    config,
		client:    client,
		statter:   statter,
		statNames: generateStatNames(config.StreamName),
	}

	w.batcher, err = batcher.New(config.Batcher, func(b [][]byte) {
		w.batches <- b
	})

	if err != nil {
		return nil, err
	}

	w.globber, err = globber.New(config.Globber, func(b []byte) {
		w.batcher.Submit(b)
	})

	if err != nil {
		return nil, err
	}

	w.Add(2)
	go w.incomingWorker()
	go w.sendWorker()
	return w, nil
}

// Write is the entry point for events into the kinesis
// writer, assuming the event is not filtered it will
// eventually be written to Kinesis as part of a flate
// compressed json blob

func (w *KinesisWriter) Write(req *WriteRequest) error {
	w.incoming <- req
	return nil
}

func (w *KinesisWriter) submit(name string, columns map[string]string) {
	event, ok := w.config.Events[name]
	if !ok {
		return
	}

	pruned := make(map[string]string)
	for _, field := range event.Fields {
		if val, ok := columns[field]; ok {
			pruned[field] = val
		} else {
			pruned[field] = ""
		}
	}

	if len(pruned) > 0 {
		w.globber.Submit(struct {
			Name   string
			Fields map[string]string
		}{
			Name:   name,
			Fields: pruned,
		})
	}
}

func (w *KinesisWriter) incomingWorker() {
	defer w.Done()

	defer func() {
		// tell the globber to flush itself
		w.globber.Close()

		// tell the batcher to flush itself
		w.batcher.Close()

		// done with the batches channel
		close(w.batches)
	}()

	for {
		req, ok := <-w.incoming
		if !ok {
			return
		}
		w.submit(req.Category, req.Record)
	}
}

func (w *KinesisWriter) sendWorker() {
	defer w.Done()

	for {
		batch, ok := <-w.batches
		if !ok {
			return
		}
		w.Add(1)
		go w.sendBatch(batch)
	}
}

const version = 1

type record struct {
	UUID    string
	Version int
	Data    []byte
}

func (w *KinesisWriter) incStat(stat int, amount int64) {
	if amount != 0 {
		err := w.statter.Inc(w.statNames[stat], amount, 1)
		if err != nil {
			logger.WithError(err).WithField("statName", w.statNames[stat]).
				Error("Failed to put stat")
		}
	}
}

func (w *KinesisWriter) sendBatch(batch [][]byte) {
	defer w.Done()
	if len(batch) == 0 {
		return
	}

	records := make([]*kinesis.PutRecordsRequestEntry, len(batch))
	for i, e := range batch {
		UUID := uuid.NewV4()
		data, _ := json.Marshal(&record{
			UUID:    UUID.String(),
			Version: version,
			Data:    e,
		})
		records[i] = &kinesis.PutRecordsRequestEntry{
			PartitionKey: aws.String(UUID.String()),
			Data:         data,
		}
	}

	retryDelay, _ := time.ParseDuration(w.config.RetryDelay)

	args := &kinesis.PutRecordsInput{
		StreamName: aws.String(w.config.StreamName),
		Records:    records,
	}

	for attempt := 1; attempt <= w.config.MaxAttemptsPerRecord; attempt++ {
		w.incStat(statPutRecordsAttempted, 1)
		w.incStat(statPutRecordsLength, int64(len(records)))

		res, err := w.client.PutRecords(args)

		if err != nil {
			logger.WithError(err).WithFields(map[string]interface{}{
				"attempt":      attempt,
				"max_attempts": w.config.MaxAttemptsPerRecord,
			}).Error("Failed to put records")
			w.incStat(statPutRecordsErrors, 1)
			time.Sleep(retryDelay)
			continue
		}

		// Find all failed records and update the slice to contain only failures
		i := 0
		var provisionThroughputExceeded, internalFailure, unknownError, succeeded int64
		for j, result := range res.Records {
			shard := aws.StringValue(result.ShardId)
			if shard == "" {
				shard = "unknown"
			}

			if aws.StringValue(result.ErrorCode) != "" {
				switch aws.StringValue(result.ErrorCode) {
				case "ProvisionedThroughputExceededException":
					provisionThroughputExceeded++
				case "InternalFailure":
					internalFailure++
				default:
					// Something undocumented
					unknownError++
				}
				args.Records[i] = args.Records[j]
				i++
			} else {
				succeeded++
			}
		}
		w.incStat(statRecordsFailedThrottled, provisionThroughputExceeded)
		w.incStat(statRecordsFailedInternalError, internalFailure)
		w.incStat(statRecordsFailedUnknown, unknownError)
		w.incStat(statRecordsSucceeded, succeeded)
		args.Records = args.Records[:i]

		if len(args.Records) == 0 {
			// No records need to be retried.
			return
		}

		time.Sleep(retryDelay)
	}
	logger.WithField("failures", len(args.Records)).
		WithField("attempts", len(records)).
		Error("Failed to send records to Kinesis")
	w.incStat(statRecordsDropped, int64(len(args.Records)))
}

// Close closes a KinesisWriter
func (w *KinesisWriter) Close() error {
	close(w.incoming)
	w.Wait()
	return nil
}

// Rotate doesn't do anything as KinesisWriters don't need to
// rotate.
func (w *KinesisWriter) Rotate() (bool, error) {
	return true, nil
}
