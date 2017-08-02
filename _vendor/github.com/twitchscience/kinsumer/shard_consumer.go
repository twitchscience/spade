// Copyright (c) 2016 Twitch Interactive

package kinsumer

import (
	"fmt"
	"log"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
	"github.com/twitchscience/aws_utils/logger"
)

const (
	// getRecordsLimit is the max number of records in a single request. This effectively limits the
	// total processing speed to getRecordsLimit*5/n where n is the number of parallel clients trying
	// to consume from the same kinesis stream
	getRecordsLimit = 10000 // 10,000 is the max according to the docs

	// maxErrorRetries is how many times we will retry on a shard error
	maxErrorRetries = 3

	// errorSleepDuration is how long we sleep when an error happens, this is multiplied by the number
	// of retries to give a minor backoff behavior
	errorSleepDuration = 1 * time.Second
)

// getShardIterator gets a shard iterator after the last sequence number we read or at the start of the stream
func getShardIterator(k kinesisiface.KinesisAPI, streamName string, shardID string, sequenceNumber string) (string, error) {
	shardIteratorType := kinesis.ShardIteratorTypeAfterSequenceNumber

	// If we do not have a sequenceNumber yet we need to get a shardIterator
	// from the horizon
	ps := aws.String(sequenceNumber)
	if sequenceNumber == "" {
		shardIteratorType = kinesis.ShardIteratorTypeTrimHorizon
		ps = nil
	}

	resp, err := k.GetShardIterator(&kinesis.GetShardIteratorInput{
		ShardId:                aws.String(shardID),
		ShardIteratorType:      &shardIteratorType,
		StartingSequenceNumber: ps,
		StreamName:             aws.String(streamName),
	})
	return aws.StringValue(resp.ShardIterator), err
}

// getRecords returns the next records and shard iterator from the given shard iterator
func getRecords(k kinesisiface.KinesisAPI, iterator string) (records []*kinesis.Record, nextIterator string, lag time.Duration, err error) {
	params := &kinesis.GetRecordsInput{
		Limit:         aws.Int64(getRecordsLimit),
		ShardIterator: aws.String(iterator),
	}

	output, err := k.GetRecords(params)
	if err != nil {
		logger.WithFields(map[string]interface{}{
			"Limit":         getRecordsLimit,
			"ShardIterator": iterator,
		}).Info("*** Additional logging for Kinesis.GetRecords() ***")
		return nil, "", 0, err
	}

	records = output.Records
	nextIterator = aws.StringValue(output.NextShardIterator)
	lag = time.Duration(aws.Int64Value(output.MillisBehindLatest)) * time.Millisecond

	return records, nextIterator, lag, nil
}

// captureShard blocks until we capture the given shardID
func (k *Kinsumer) captureShard(shardID string) (*checkpointer, error) {
	// Attempt to capture the shard in dynamo
	for {
		// Ask the checkpointer to capture the shard
		checkpointer, err := capture(
			shardID,
			k.checkpointTableName,
			k.dynamodb,
			k.clientName,
			k.clientID,
			k.maxAgeForClientRecord,
			k.config.stats)
		if err != nil {
			return nil, err
		}

		if checkpointer != nil {
			return checkpointer, nil
		}

		// Throttle requests so that we don't hammer dynamo
		select {
		case <-k.stop:
			// If we are told to stop consuming we should stop attempting to capture
			return nil, nil
		case <-time.After(k.config.throttleDelay):
		}
	}
}

// consume is a blocking call that captures then consumes the given shard in a loop.
// It is also responsible for writing out the checkpoint updates to dynamo.
// TODO: There are no tests for this file. Not sure how to even unit test this.
func (k *Kinsumer) consume(shardID string) {
	defer k.waitGroup.Done()

	// commitTicker is used to periodically commit, so that we don't hammer dynamo every time
	// a shard wants to be check pointed
	commitTicker := time.NewTicker(k.config.commitFrequency)
	defer commitTicker.Stop()

	// capture the checkpointer
	checkpointer, err := k.captureShard(shardID)
	captureTime := time.Now()
	if err != nil {
		k.shardErrors <- shardConsumerError{shardID: shardID, action: "captureShard", err: err}
		return
	}

	// if we failed to capture the checkpointer but there was no errors
	// we must have stopped, so don't process this shard at all
	if checkpointer == nil {
		return
	}

	sequenceNumber := checkpointer.sequenceNumber

	// finished means we have reached the end of the shard but haven't necessarily processed/committed everything
	finished := false
	// Make sure we release the shard when we are done.
	defer func() {
		now := time.Now()
		d := now.Sub(captureTime)
		if d > k.maxAgeForClientRecord {
			// Grab the entry from dynamo assuming there is one
			resp, err := checkpointer.dynamodb.GetItem(&dynamodb.GetItemInput{
				TableName:      aws.String(checkpointer.tableName),
				ConsistentRead: aws.Bool(true),
				Key: map[string]*dynamodb.AttributeValue{
					"Shard": {S: aws.String(checkpointer.shardID)},
				},
			})

			if err != nil {
				logger.Info(fmt.Sprintf("Error calling GetItem on shard checkpoint: %v", err))
				return
			}

			// Convert to struct so we can work with the values
			var record checkpointRecord
			if err = dynamodbattribute.ConvertFromMap(resp.Item, &record); err != nil {
				logger.Info(fmt.Sprintf("Error converting DynamoDB item: %v", err))
				return
			}

			var sequenceNumber string
			var ownerName string
			var finished int64
			var ownerID string
			var finishedRFC string

			if record.SequenceNumber != nil {
				sequenceNumber = *record.SequenceNumber
			}
			if record.OwnerName != nil {
				ownerName = *record.OwnerName
			}
			if record.Finished != nil {
				finished = *record.Finished
			}
			if record.OwnerID != nil {
				ownerID = *record.OwnerID
			}
			if record.FinishedRFC != nil {
				finishedRFC = *record.FinishedRFC
			}

			logger.WithFields(map[string]interface{}{
				"TableName":             checkpointer.tableName,
				"Shard":                 record.Shard,
				"SequenceNumber":        sequenceNumber,
				"LastUpdate":            record.LastUpdate,
				"OwnerName":             ownerName,
				"Finished":              finished,
				"OwnerID":               ownerID,
				"LastUpdateRFC":         record.LastUpdateRFC,
				"FinishedRFC":           finishedRFC,
				"captureTime":           captureTime.Format(time.RFC1123Z),
				"releaseTime":           now.Format(time.RFC1123Z),
				"duration":              d.Seconds(),
				"maxAgeForClientRecord": k.maxAgeForClientRecord,
			}).Info("*** kinsumer.consume(): Time between capturing a shard and releasing it greater than maxAgeForClientRecord threshold  ***")
		}
		innerErr := checkpointer.release()
		if innerErr != nil {
			k.shardErrors <- shardConsumerError{shardID: shardID, action: "checkpointer.release", err: innerErr}
			return
		}
	}()

	// Get the starting shard iterator
	iterator, err := getShardIterator(k.kinesis, k.streamName, shardID, sequenceNumber)
	if err != nil {
		k.shardErrors <- shardConsumerError{shardID: shardID, action: "getShardIterator", err: err}
		return
	}

	// no throttle on the first request.
	nextThrottle := time.After(0)

	retryCount := 0

	lastNow := time.Now()

	var lastSeqNum string

mainloop:
	for {
		// We have reached the end of the shard's data. Set Finished in dynamo and stop processing.
		if iterator == "" && !finished {
			checkpointer.finish(lastSeqNum)
			finished = true
		}

		// Handle async actions, and throttle requests to keep kinesis happy
		select {
		case <-k.stop:
			return
		case <-commitTicker.C:
			now := time.Now()
			d := now.Sub(lastNow)
			if d > k.config.shardCheckFrequency {
				logger.WithFields(map[string]interface{}{
					"shardID":             shardID,
					"lastNow":             lastNow.Format(time.RFC1123Z),
					"now":                 now.Format(time.RFC1123Z),
					"duration":            d.Seconds(),
					"shardCheckFrequency": k.config.shardCheckFrequency,
				}).Info("Time between commitTicker.C greater than the shardCheckFrequency threshold")
			}
			lastNow = now

			finishCommitted, err := checkpointer.commit()
			if err != nil {
				k.shardErrors <- shardConsumerError{shardID: shardID, action: "checkpointer.commit", err: err}
				return
			}
			if finishCommitted {
				return
			}
			// Go back to waiting for a throttle/stop.
			continue mainloop
		case <-nextThrottle:
		}

		// Reset the nextThrottle
		nextThrottle = time.After(k.config.throttleDelay)

		if finished {
			continue mainloop
		}

		// Get records from kinesis
		records, next, lag, err := getRecords(k.kinesis, iterator)

		if err != nil {
			if awsErr, ok := err.(awserr.Error); ok {
				log.Printf("Got error: %s (%s) retry count is %d / %d", awsErr.Message(), awsErr.OrigErr(), retryCount, maxErrorRetries)
				if retryCount < maxErrorRetries {
					retryCount++

					// casting retryCount here to time.Duration purely for the multiplication, there is
					// no meaning to retryCount nanoseconds
					time.Sleep(errorSleepDuration * time.Duration(retryCount))
					continue mainloop
				}
			}
			k.shardErrors <- shardConsumerError{shardID: shardID, action: "getRecords", err: err}
			return
		}
		retryCount = 0

		// Put all the records we got onto the channel
		k.config.stats.EventsFromKinesis(len(records), shardID, lag)
		if len(records) > 0 {
			retrievedAt := time.Now()
			for _, record := range records {
				select {
				case <-k.stop:
					return
				case k.records <- &consumedRecord{
					record:       record,
					checkpointer: checkpointer,
					retrievedAt:  retrievedAt,
				}:
				}
			}

			// Update the last sequence number we saw, in case we reached the end of the stream.
			lastSeqNum = aws.StringValue(records[len(records)-1].SequenceNumber)
		}
		iterator = next
	}
}
