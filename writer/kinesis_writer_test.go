package writer

import (
	"encoding/json"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/firehose"
	"github.com/aws/aws-sdk-go/service/firehose/firehoseiface"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
	"github.com/cactus/go-statsd-client/statsd"
	"github.com/cactus/go-statsd-client/statsd/statsdtest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twitchscience/scoop_protocol/scoop_protocol"
)

var FirehoseRedshiftStreamTestConfig = []byte(`
    {
        "StreamName": "spade-processed-integration-jackgao-coview-redshift-test",
        "StreamType": "firehose",
        "Compress": false,
        "FirehoseRedshiftStream": true,
        "Events": {
            "minute-watched": {
                "Fields": [
                    "country",
                    "device_id"
                ]
            },
            "video-play": {
                "Fields": [
                    "country",
                    "device_id",
		    "game"
                ]
            }
        },
        "BufferSize": 1024,
        "MaxAttemptsPerRecord": 1,
        "RetryDelay": "1s",
        "Globber": {
            "MaxSize": 990000,
            "MaxAge": "1s",
            "BufferLength": 1024
        },
        "Batcher": {
            "MaxSize": 990000,
            "MaxEntries": 500,
            "MaxAge": "1s",
            "BufferLength": 1024
        }
    }
  `)

// mocking firehoseAPI
type firehoseMock struct {
	received []map[string]string
	response *firehose.PutRecordBatchOutput
	firehoseiface.FirehoseAPI
}

func (f *firehoseMock) PutRecordBatch(i *firehose.PutRecordBatchInput) (*firehose.PutRecordBatchOutput, error) {
	for _, v := range i.Records {
		var unpacked map[string]string
		_ = json.Unmarshal(v.Data, &unpacked)
		f.received = append(f.received, unpacked)
	}
	return f.response, nil
}

// mocking KinesisAPI
type kinesisMock struct {
	received []map[string]string
	response *kinesis.PutRecordsOutput
	kinesisiface.KinesisAPI
}

func (k *kinesisMock) PutRecords(i *kinesis.PutRecordsInput) (*kinesis.PutRecordsOutput, error) {
	for _, v := range i.Records {
		var unpacked jsonRecord
		_ = json.Unmarshal(v.Data, &unpacked)
		k.received = append(k.received, unpacked.Data)
	}
	return k.response, nil
}

func TestConfigValidation(t *testing.T) {
	config := scoop_protocol.KinesisWriterConfig{}
	_ = json.Unmarshal(FirehoseRedshiftStreamTestConfig, &config)
	assert.Nil(t, config.Validate(), "config could not be validated")
}

func TestRedshiftStreamAndCompressValidation(t *testing.T) {
	config := scoop_protocol.KinesisWriterConfig{}
	_ = json.Unmarshal(FirehoseRedshiftStreamTestConfig, &config)
	config.Compress = true

	// firehose->redshift streaming cannot be used with compress mode
	assert.NotNil(t, config.Validate(), "redshift streaming and compress cannot both be on")
}

type forwarderMock struct {
	received [][]byte
}

func (f *forwarderMock) Submit(e []byte) {
	f.received = append(f.received, e)
}

func (f *forwarderMock) Close() {}

func TestSubmitCompressed(t *testing.T) {
	config := scoop_protocol.KinesisWriterConfig{}
	_ = json.Unmarshal(FirehoseRedshiftStreamTestConfig, &config)
	config.Compress = true
	globber := forwarderMock{}
	batcher := forwarderMock{}
	k := KinesisWriter{
		globber: &globber,
		batcher: &batcher,
		config:  config,
	}
	k.submit("minute-watched", map[string]string{"country": "US", "something": "xx"})
	assert.Len(t, batcher.received, 0)
	require.Len(t, globber.received, 1)
	assert.Equal(t, `{"Name":"minute-watched","Fields":{"country":"US","device_id":""}}`,
		string(globber.received[0]))
}

func TestSubmitUncompressed(t *testing.T) {
	config := scoop_protocol.KinesisWriterConfig{}
	_ = json.Unmarshal(FirehoseRedshiftStreamTestConfig, &config)
	globber := forwarderMock{}
	batcher := forwarderMock{}
	k := KinesisWriter{
		globber: &globber,
		batcher: &batcher,
		config:  config,
	}
	k.submit("minute-watched", map[string]string{"country": "US", "something": "xx"})
	assert.Len(t, globber.received, 0)
	require.Len(t, batcher.received, 1)
	assert.Equal(t, `{"country":"US","device_id":""}`, string(batcher.received[0]))
}

func TestSubmitUncompressedEventName(t *testing.T) {
	config := scoop_protocol.KinesisWriterConfig{}
	_ = json.Unmarshal(FirehoseRedshiftStreamTestConfig, &config)
	config.EventNameTargetField = "event"
	globber := forwarderMock{}
	batcher := forwarderMock{}
	k := KinesisWriter{
		globber: &globber,
		batcher: &batcher,
		config:  config,
	}
	k.submit("minute-watched", map[string]string{"country": "US", "something": "xx"})
	assert.Len(t, globber.received, 0)
	require.Len(t, batcher.received, 1)
	assert.Equal(t, `{"country":"US","device_id":"","event":"minute-watched"}`,
		string(batcher.received[0]))
}

func TestSubmitUncompressedExcludeEmpty(t *testing.T) {
	config := scoop_protocol.KinesisWriterConfig{}
	_ = json.Unmarshal(FirehoseRedshiftStreamTestConfig, &config)
	config.ExcludeEmptyFields = true
	globber := forwarderMock{}
	batcher := forwarderMock{}
	k := KinesisWriter{
		globber: &globber,
		batcher: &batcher,
		config:  config,
	}
	k.submit("video-play", map[string]string{"country": "US", "device_id": "", "something": "xx"})
	assert.Len(t, globber.received, 0)
	require.Len(t, batcher.received, 1)
	assert.Equal(t, `{"country":"US"}`, string(batcher.received[0]))
}

func TestRedshiftStreamAndStreamValidation(t *testing.T) {
	config := scoop_protocol.KinesisWriterConfig{}
	_ = json.Unmarshal(FirehoseRedshiftStreamTestConfig, &config)
	config.StreamType = "stream"

	// firehose->redshift streaming can only be used with firehose
	assert.NotNil(t, config.Validate(), "redshift streaming can only be used with firehose")
}

func TestStream(t *testing.T) {
	inputMaps := []map[string]string{
		{"country": "US", "device_id": "xyz123"},
		{"country": "", "device_id": ""},
		{"country": "CA", "device_id": "\x00"},
		{"country": "CA", "device_id": "xyz\x00123"},
	}
	config := scoop_protocol.KinesisWriterConfig{}
	_ = json.Unmarshal(FirehoseRedshiftStreamTestConfig, &config)
	config.StreamType = "stream"
	config.FirehoseRedshiftStream = false

	// create mock objects
	mockStatter := &Statter{
		statter:   &statsd.NoopClient{},
		statNames: map[int]string{},
	}
	mockKinesis := kinesisMock{response: &kinesis.PutRecordsOutput{}}
	writer := &StreamBatchWriter{&mockKinesis, &config, mockStatter}

	// matching input format
	inputBatch := [][]byte{}
	for _, m := range inputMaps {
		b, _ := json.Marshal(m)
		inputBatch = append(inputBatch, b)
	}

	// send it
	writer.SendBatch(inputBatch)

	// check for expected values, which exactly match input values
	for i := range mockKinesis.received {
		for k := range inputMaps[i] {
			assert.Equal(t, inputMaps[i][k], mockKinesis.received[i][k],
				"output was not equal for input %d key %s", i, k)
			delete(mockKinesis.received[i], k)
		}
		for k := range mockKinesis.received[i] {
			assert.Fail(t, "Extra value", "Unexpected key %s for input %d", k, i)
		}
	}
}

func TestRedshiftStreamMode(t *testing.T) {
	inputMaps := []map[string]string{
		{"country": "US", "device_id": "xyz123"},
		{"country": "", "device_id": ""},
		{"country": "CA", "device_id": "\x00"},
		{"country": "CA", "device_id": "xyz\x00123"},
		{"country": "CA", "device_id": "\x00\x00\x00\x00\x00"},
	}

	expectedMaps := []map[string]string{
		{"country": "US", "device_id": "xyz123"},
		{"country": "", "device_id": ""},
		{"country": "CA", "device_id": ""},
		{"country": "CA", "device_id": "xyz123"},
		{"country": "CA", "device_id": ""},
	}

	config := scoop_protocol.KinesisWriterConfig{}
	_ = json.Unmarshal(FirehoseRedshiftStreamTestConfig, &config)

	// create mock objects
	mockStatter := &Statter{
		statter:   &statsd.NoopClient{},
		statNames: map[int]string{},
	}
	mockFirehose := firehoseMock{response: &firehose.PutRecordBatchOutput{}}
	batchWriter := &FirehoseBatchWriter{&mockFirehose, &config, mockStatter}

	// matching input format
	inputBatch := [][]byte{}
	for _, m := range inputMaps {
		b, _ := json.Marshal(m)
		inputBatch = append(inputBatch, b)
	}

	// send it
	batchWriter.SendBatch(inputBatch)

	// check for expected values
	for i := range mockFirehose.received {
		for k := range expectedMaps[i] {
			assert.Equal(t, expectedMaps[i][k], mockFirehose.received[i][k],
				"output was not equal for input %d key %s", i, k)
			delete(mockFirehose.received[i], k)
		}
		for k := range mockFirehose.received[i] {
			assert.Fail(t, "Extra value", "Unexpected key %s for input %d", k, i)
		}
	}
}

func TestRedshiftStreamStatting(t *testing.T) {
	inputMaps := []map[string]string{
		{"country": "US", "device_id": "xyz123"},
		{"country": "", "device_id": ""},
	}

	config := scoop_protocol.KinesisWriterConfig{}
	_ = json.Unmarshal(FirehoseRedshiftStreamTestConfig, &config)

	// create mock objects
	statRecorder := statsdtest.NewRecordingSender()
	statsdClient, _ := statsd.NewClientWithSender(statRecorder, "")
	mockStatter := &Statter{
		statter:   statsdClient,
		statNames: generateStatNames("stream"),
	}
	mockFirehose := firehoseMock{response: &firehose.PutRecordBatchOutput{
		RequestResponses: []*firehose.PutRecordBatchResponseEntry{
			{ErrorCode: aws.String("ServiceUnavailableException")},
			{ErrorCode: aws.String("InternalFailure")},
		},
	}}
	batchWriter := &FirehoseBatchWriter{&mockFirehose, &config, mockStatter}

	// matching input format
	inputBatch := [][]byte{}
	for _, m := range inputMaps {
		b, _ := json.Marshal(m)
		inputBatch = append(inputBatch, b)
	}

	// send it
	batchWriter.SendBatch(inputBatch)

	stats := statRecorder.GetSent()
	require.Equal(t, 5, len(stats))
	assert.Equal(t, "kinesiswriter.stream.putrecords.attempted 1 ", stats[0].String())
	assert.Equal(t, "kinesiswriter.stream.putrecords.length 2 ", stats[1].String())
	assert.Equal(t, "kinesiswriter.stream.records_failed.internal_error 1 ", stats[2].String())
	assert.Equal(t, "kinesiswriter.stream.records_failed.unknown_reason 1 ", stats[3].String())
	assert.Equal(t, "kinesiswriter.stream.records_dropped 2 ", stats[4].String())
}
