package writer

import (
	"encoding/json"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/firehose"
	"github.com/aws/aws-sdk-go/service/firehose/firehoseiface"
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

func TestRedshiftStreamAndStreamValidation(t *testing.T) {
	config := scoop_protocol.KinesisWriterConfig{}
	_ = json.Unmarshal(FirehoseRedshiftStreamTestConfig, &config)
	config.StreamType = "stream"

	// firehose->redshift streaming can only be used with firehose
	assert.NotNil(t, config.Validate(), "redshift streaming can only be used with firehose")
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
		for k := range mockFirehose.received[i] {
			assert.Equal(t, expectedMaps[i][k], mockFirehose.received[i][k],
				"output was not equal for input %v key %s", i, k)
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
