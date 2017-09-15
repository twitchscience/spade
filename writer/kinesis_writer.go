package writer

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"golang.org/x/time/rate"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials/stscreds"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/firehose"
	"github.com/aws/aws-sdk-go/service/firehose/firehoseiface"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
	"github.com/cactus/go-statsd-client/statsd"
	"github.com/myesui/uuid"
	"github.com/twitchscience/aws_utils/logger"
	"github.com/twitchscience/scoop_protocol/scoop_protocol"
	"github.com/twitchscience/spade/batcher"
	"github.com/twitchscience/spade/globber"
)

const (
	// RedshiftDatetimeIngestString is the format of timestamps that Redshift understands.
	RedshiftDatetimeIngestString = "2006-01-02 15:04:05.999"
)

// Statter sends stats for a BatchWriter.
type Statter struct {
	statter   statsd.Statter
	statNames map[int]string
}

// NewStatter returns a Statter for the given stream.
func NewStatter(statter statsd.Statter, streamName string) *Statter {
	return &Statter{
		statter:   statter,
		statNames: generateStatNames(streamName),
	}
}

// IncStat increments a stat by an amount on the Statter.
func (w *Statter) IncStat(stat int, amount int64) {
	if amount != 0 {
		err := w.statter.Inc(w.statNames[stat], amount, 1)
		if err != nil {
			logger.WithError(err).WithField("statName", w.statNames[stat]).
				Error("Failed to put stat")
		}
	}
}

// EventForwarder receives events and forwards them to Kinesis or another EventForwarder.
type EventForwarder interface {
	Submit([]byte)
	Close()
}

// BatchWriter is an interface to write batches to an external sink.
type BatchWriter interface {
	SendBatch([][]byte)
}

// taskRateLimiter receives tasks as closures and executes them if they fall within the
// rate limit specified at construction. Otherwise it drops the tasks on the floor.
type taskRateLimiter struct {
	rateLimiter *rate.Limiter
}

func newTaskRateLimiter(tasksBeforeThrottling int, secondsPerThrottledTask int64) *taskRateLimiter {
	return &taskRateLimiter{rate.NewLimiter(
		rate.Every(time.Duration(secondsPerThrottledTask)*time.Second),
		tasksBeforeThrottling),
	}
}

func (t *taskRateLimiter) attempt(f func()) {
	if t.rateLimiter.Allow() {
		f()
	}
}

func formatErrorCounts(errorCounts map[string]int) error {
	keys := make([]string, 0, len(errorCounts))
	for k := range errorCounts {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	pairs := make([]string, 0, len(keys))
	for _, k := range keys {
		pairs = append(pairs, fmt.Sprintf("%v: %v", k, errorCounts[k]))
	}

	return errors.New(strings.Join(pairs, ", "))
}

// StreamBatchWriter writes batches to Kinesis Streams
type StreamBatchWriter struct {
	client  kinesisiface.KinesisAPI
	config  *scoop_protocol.KinesisWriterConfig
	statter *Statter
	limiter *taskRateLimiter
}

// FirehoseBatchWriter writes batches to Kinesis Firehose
type FirehoseBatchWriter struct {
	client  firehoseiface.FirehoseAPI
	config  *scoop_protocol.KinesisWriterConfig
	statter *Statter
	limiter *taskRateLimiter
}

// KinesisWriter is a writer that writes events to kinesis
type KinesisWriter struct {
	incoming    chan *WriteRequest
	batches     chan [][]byte
	globber     EventForwarder
	batcher     EventForwarder
	config      scoop_protocol.KinesisWriterConfig
	batchWriter BatchWriter
	limiter     *taskRateLimiter

	sync.WaitGroup
}

// KinesisFactory returns a kinesis interface from a given session.
type KinesisFactory interface {
	New(region, role string) kinesisiface.KinesisAPI
}

// FirehoseFactory returns a firehose interface from a given session.
type FirehoseFactory interface {
	New(region, role string) firehoseiface.FirehoseAPI
}

// DefaultKinesisFactory returns a normal Kinesis client.
type DefaultKinesisFactory struct {
	Session *session.Session
}

// DefaultFirehoseFactory returns a normal Firehose client.
type DefaultFirehoseFactory struct {
	Session *session.Session
}

func getConfiguredSession(session *session.Session, region, role string) *session.Session {
	if role != "" {
		credentials := stscreds.NewCredentials(
			session,
			role,
			func(provider *stscreds.AssumeRoleProvider) {
				provider.ExpiryWindow = time.Minute
			})
		return session.Copy(&aws.Config{
			Credentials: credentials, Region: aws.String(region)})
	} else if region != "" && region != aws.StringValue(session.Config.Region) {
		return session.Copy(&aws.Config{Region: aws.String(region)})
	}
	return session
}

// New returns a kinesis client configured to use the given region/role.
func (f *DefaultKinesisFactory) New(region, role string) kinesisiface.KinesisAPI {
	return kinesis.New(getConfiguredSession(f.Session, region, role))
}

// New returns a firehose client configured to use the given region/role.
func (f *DefaultFirehoseFactory) New(region, role string) firehoseiface.FirehoseAPI {
	return firehose.New(getConfiguredSession(f.Session, region, role))
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
func NewKinesisWriter(
	kinesisFactory KinesisFactory,
	firehoseFactory FirehoseFactory,
	statter statsd.Statter,
	config scoop_protocol.KinesisWriterConfig,
	errorsBeforeThrottling int,
	secondsPerError int64) (SpadeWriter, error) {

	if err := config.Validate(); err != nil {
		return nil, err
	}
	var batchWriter BatchWriter
	wStatter := NewStatter(statter, config.StreamName)
	limiter := newTaskRateLimiter(errorsBeforeThrottling, secondsPerError)
	switch config.StreamType {
	case "stream":
		batchWriter = &StreamBatchWriter{kinesisFactory.New(config.StreamRegion, config.StreamRole), &config, wStatter, limiter}
	case "firehose":
		batchWriter = &FirehoseBatchWriter{firehoseFactory.New(config.StreamRegion, config.StreamRole), &config, wStatter, limiter}
	default:
		return nil, fmt.Errorf("unknown stream type: %s", config.StreamType)
	}
	w := &KinesisWriter{
		incoming:    make(chan *WriteRequest, config.BufferSize),
		batches:     make(chan [][]byte),
		config:      config,
		batchWriter: batchWriter,
	}

	var err error
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
	logger.Go(w.incomingWorker)
	logger.Go(w.sendWorker)

	return w, nil
}

// Write is the entry point for events into the kinesis
// writer, assuming the event is not filtered it will
// eventually be written to Kinesis as part of a flate
// compressed json blob

func (w *KinesisWriter) Write(req *WriteRequest) {
	w.incoming <- req
}

func (w *KinesisWriter) submit(name string, columns map[string]string) {
	event, ok := w.config.Events[name]
	if !ok {
		return
	}
	if event.FilterFunc != nil && !event.FilterFunc(columns) {
		return
	}

	pruned := make(map[string]string)
	if w.config.EventNameTargetField != "" {
		pruned[w.config.EventNameTargetField] = name
	}
	for field, outField := range event.FullFieldMap {
		if val, ok := columns[field]; ok && val != "" {
			pruned[outField] = val
		} else if !w.config.ExcludeEmptyFields {
			pruned[outField] = ""
		}
	}

	if len(pruned) > 0 {
		// if we want data compressed, we send it to globber
		if w.config.Compress {
			entry := Event{
				Name:   name,
				Fields: pruned,
			}
			b, err := json.Marshal(entry)
			if err != nil {
				w.limiter.attempt(func() {
					logger.WithError(err).WithField("name", name).Error(
						"Failed to marshal Kinesis entry for globber")
				})
				return
			}
			w.globber.Submit(b)
		} else {
			e, err := json.Marshal(pruned)
			if err != nil {
				w.limiter.attempt(func() {
					logger.WithError(err).WithField("name", name).Error(
						"Failed to marshal Kinesis entry for batcher")
				})
				return
			}
			w.batcher.Submit(e)
		}
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
		logger.Go(func() {
			defer w.Done()
			w.batchWriter.SendBatch(batch)
		})
	}
}

const version = 1

// Record is a compressed record to be sent to Kinesis.
type Record struct {
	UUID      string
	Version   int
	Data      []byte
	CreatedAt string
}

// JSONRecord is a raw JSON record to be sent to Kinesis.
type JSONRecord struct {
	UUID      string
	Version   int
	Data      map[string]string
	CreatedAt string
}

// Event is a collection of name and fields for compressed streams.
type Event struct {
	Name   string
	Fields map[string]string
}

// SendBatch writes the given batch to a stream, configured by the KinesisWriter
func (w *StreamBatchWriter) SendBatch(batch [][]byte) {
	if len(batch) == 0 {
		return
	}

	records := make([]*kinesis.PutRecordsRequestEntry, 0, len(batch))
	for _, e := range batch {
		records = append(records, w.putRecordsRequestEntry(e))
	}

	retryDelay, _ := time.ParseDuration(w.config.RetryDelay)
	args := &kinesis.PutRecordsInput{
		StreamName: aws.String(w.config.StreamName),
		Records:    records,
	}

	var err error
	for attempt := 1; attempt <= w.config.MaxAttemptsPerRecord; attempt++ {
		err = w.attemptPutRecords(args)
		if err == nil {
			return
		}

		logger.WithError(err).WithFields(map[string]interface{}{
			"attempt":      attempt,
			"max_attempts": w.config.MaxAttemptsPerRecord,
			"stream":       w.config.StreamName,
		}).Warn("Failed to put records")

		if attempt < w.config.MaxAttemptsPerRecord {
			time.Sleep(retryDelay)
		}
	}

	w.limiter.attempt(func() {
		logger.WithError(err).WithFields(map[string]interface{}{
			"failures": len(args.Records),
			"attempts": len(records),
			"stream":   w.config.StreamName,
		}).Error("Failed to send records to Kinesis")
	})
	w.statter.IncStat(statRecordsDropped, int64(len(args.Records)))
}

func (w *StreamBatchWriter) putRecordsRequestEntry(eventData []byte) *kinesis.PutRecordsRequestEntry {
	UUIDString := uuid.NewV4().String()

	var data []byte
	var marshalErr error
	var unmarshalErr error
	if w.config.Compress {
		// if we are sending compressed data, send it as is
		data, marshalErr = json.Marshal(&Record{
			UUID:      UUIDString,
			Version:   version,
			Data:      eventData,
			CreatedAt: time.Now().UTC().Format(RedshiftDatetimeIngestString),
		})
	} else {
		// if sending uncompressed json, remarshal batch into new objects
		var unpacked map[string]string
		unmarshalErr = json.Unmarshal(eventData, &unpacked)
		data, marshalErr = json.Marshal(&JSONRecord{
			UUID:      UUIDString,
			Version:   version,
			Data:      unpacked,
			CreatedAt: time.Now().UTC().Format(RedshiftDatetimeIngestString),
		})
	}

	if marshalErr != nil {
		w.limiter.attempt(func() {
			logger.WithField("stream", w.config.StreamName).
				WithError(marshalErr).
				Error("Failed to marshal into Record")
		})
	}

	if unmarshalErr != nil {
		w.limiter.attempt(func() {
			logger.WithField("stream", w.config.StreamName).
				WithError(unmarshalErr).
				Error("Failed to unmarshal into Record")
		})
	}

	return &kinesis.PutRecordsRequestEntry{
		PartitionKey: aws.String(UUIDString),
		Data:         data,
	}
}

func (w *StreamBatchWriter) attemptPutRecords(args *kinesis.PutRecordsInput) error {
	w.statter.IncStat(statPutRecordsAttempted, 1)
	w.statter.IncStat(statPutRecordsLength, int64(len(args.Records)))

	res, err := w.client.PutRecords(args)
	if err != nil {
		return err
	}

	// Find all failed records and update the slice to contain only failures
	retryCount := 0
	var provisionThroughputExceeded, internalFailure, unknownError, succeeded int64
	awsErrorCounts := make(map[string]int)
	for j, result := range res.Records {
		awsError := aws.StringValue(result.ErrorCode)
		if awsError == "" {
			succeeded++
			continue
		}

		awsErrorCounts[awsError]++
		switch awsError {
		// Kinesis stream throttling
		case "ProvisionedThroughputExceededException":
			provisionThroughputExceeded++
			// Firehose throttling
		case "ServiceUnavailableException":
			provisionThroughputExceeded++
		case "InternalFailure":
			internalFailure++
		default:
			// Something undocumented
			unknownError++
		}
		args.Records[retryCount] = args.Records[j]
		retryCount++
	}

	w.statter.IncStat(statRecordsFailedThrottled, provisionThroughputExceeded)
	w.statter.IncStat(statRecordsFailedInternalError, internalFailure)
	w.statter.IncStat(statRecordsFailedUnknown, unknownError)
	w.statter.IncStat(statRecordsSucceeded, succeeded)
	args.Records = args.Records[:retryCount]

	if retryCount == 0 {
		return nil
	}
	return formatErrorCounts(awsErrorCounts)
}

// SendBatch writes the given batch to a firehose, configured by the FirehoseBatchWriter
func (w *FirehoseBatchWriter) SendBatch(batch [][]byte) {
	if len(batch) == 0 {
		return
	}

	records := make([]*firehose.Record, 0, len(batch))
	for _, e := range batch {
		records = append(records, w.firehoseRecord(e))
	}

	retryDelay, _ := time.ParseDuration(w.config.RetryDelay)
	args := &firehose.PutRecordBatchInput{
		DeliveryStreamName: aws.String(w.config.StreamName),
		Records:            records,
	}

	var err error
	for attempt := 1; attempt <= w.config.MaxAttemptsPerRecord; attempt++ {
		err = w.attemptPutRecord(args)
		if err == nil {
			return
		}

		logger.WithError(err).WithFields(map[string]interface{}{
			"attempt":      attempt,
			"max_attempts": w.config.MaxAttemptsPerRecord,
			"stream":       w.config.StreamName,
		}).Warn("Failed to put some records")

		if attempt < w.config.MaxAttemptsPerRecord {
			time.Sleep(retryDelay)
		}
	}

	w.limiter.attempt(func() {
		logger.WithError(err).WithFields(map[string]interface{}{
			"failures": len(args.Records),
			"attempts": len(records),
			"stream":   w.config.StreamName,
		}).Error("Failed to send records to Firehose")
	})
	w.statter.IncStat(statRecordsDropped, int64(len(args.Records)))
}

func (w *FirehoseBatchWriter) firehoseRecord(eventData []byte) *firehose.Record {
	UUIDString := uuid.NewV4().String()

	var data []byte
	var marshalErr error
	var unmarshalErr error
	if w.config.Compress {
		// if we are sending compressed data, send it as is
		data, marshalErr = json.Marshal(&Record{
			UUID:    UUIDString,
			Version: version,
			Data:    eventData,
		})
	} else if w.config.FirehoseRedshiftStream {
		// if streaming data to Redshift, send data as top level JSON and scrub away null bytes
		var unpacked map[string]string
		unmarshalErr = json.Unmarshal(eventData, &unpacked)
		for k, v := range unpacked {
			unpacked[k] = strings.Replace(v, "\x00", "", -1)
		}
		data, marshalErr = json.Marshal(unpacked)
	} else {
		// if sending uncompressed json, remarshal batch into new objects
		var unpacked map[string]string
		unmarshalErr = json.Unmarshal(eventData, &unpacked)
		data, marshalErr = json.Marshal(&JSONRecord{
			UUID:    UUIDString,
			Version: version,
			Data:    unpacked,
		})
	}

	if marshalErr != nil {
		w.limiter.attempt(func() {
			logger.WithField("firehose", w.config.StreamName).
				WithError(marshalErr).
				Error("Failed to marshal into Record")
		})
	}

	if unmarshalErr != nil {
		w.limiter.attempt(func() {
			logger.WithField("firehose", w.config.StreamName).
				WithError(unmarshalErr).
				Error("Failed to unmarshal into Record")
		})
	}

	// Add '\n' as a record separator
	return &firehose.Record{Data: append(data, '\n')}
}

func (w *FirehoseBatchWriter) attemptPutRecord(args *firehose.PutRecordBatchInput) error {
	w.statter.IncStat(statPutRecordsAttempted, 1)
	w.statter.IncStat(statPutRecordsLength, int64(len(args.Records)))

	res, err := w.client.PutRecordBatch(args)
	if err != nil {
		return err
	}

	// Find all failed records and update the slice to contain only failures
	retryCount := 0
	var provisionThroughputExceeded, internalFailure, unknownError, succeeded int64
	awsErrorCounts := make(map[string]int)
	for j, result := range res.RequestResponses {
		awsError := aws.StringValue(result.ErrorCode)
		if awsError == "" {
			succeeded++
			continue
		}

		awsErrorCounts[awsError]++
		switch awsError {
		case "ProvisionedThroughputExceededException":
			provisionThroughputExceeded++
		case "InternalFailure":
			internalFailure++
		default:
			// Something undocumented
			unknownError++
		}
		args.Records[retryCount] = args.Records[j]
		retryCount++
	}

	w.statter.IncStat(statRecordsFailedThrottled, provisionThroughputExceeded)
	w.statter.IncStat(statRecordsFailedInternalError, internalFailure)
	w.statter.IncStat(statRecordsFailedUnknown, unknownError)
	w.statter.IncStat(statRecordsSucceeded, succeeded)
	args.Records = args.Records[:retryCount]

	if retryCount == 0 {
		return nil
	}
	return formatErrorCounts(awsErrorCounts)
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
