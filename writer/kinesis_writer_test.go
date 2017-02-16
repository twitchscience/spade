package writer

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/firehose"
	"github.com/cactus/go-statsd-client/statsd"
	"github.com/stretchr/testify/assert"
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
        "MaxAttemptsPerRecord": 10,
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

// mocking statsd's Statter
type statterMock struct{}

func (s statterMock) Inc(string, int64, float32) error                    { return nil }
func (s statterMock) Dec(string, int64, float32) error                    { return nil }
func (s statterMock) Gauge(string, int64, float32) error                  { return nil }
func (s statterMock) GaugeDelta(string, int64, float32) error             { return nil }
func (s statterMock) Timing(string, int64, float32) error                 { return nil }
func (s statterMock) TimingDuration(string, time.Duration, float32) error { return nil }
func (s statterMock) Set(string, string, float32) error                   { return nil }
func (s statterMock) SetInt(string, int64, float32) error                 { return nil }
func (s statterMock) Raw(string, string, float32) error                   { return nil }
func (s statterMock) NewSubStatter(string) statsd.SubStatter              { return nil }
func (s statterMock) SetPrefix(string)                                    {}
func (s statterMock) Close() error                                        { return nil }

// mocking firehoseAPI
type firehoseMock struct {
	received []map[string]string
}

func (f *firehoseMock) CreateDeliveryStreamRequest(*firehose.CreateDeliveryStreamInput) (*request.Request, *firehose.CreateDeliveryStreamOutput) {
	return nil, nil
}
func (f *firehoseMock) CreateDeliveryStream(*firehose.CreateDeliveryStreamInput) (*firehose.CreateDeliveryStreamOutput, error) {
	return nil, nil
}
func (f *firehoseMock) DeleteDeliveryStreamRequest(*firehose.DeleteDeliveryStreamInput) (*request.Request, *firehose.DeleteDeliveryStreamOutput) {
	return nil, nil
}
func (f *firehoseMock) DeleteDeliveryStream(*firehose.DeleteDeliveryStreamInput) (*firehose.DeleteDeliveryStreamOutput, error) {
	return nil, nil
}
func (f *firehoseMock) DescribeDeliveryStreamRequest(*firehose.DescribeDeliveryStreamInput) (*request.Request, *firehose.DescribeDeliveryStreamOutput) {
	return nil, nil
}
func (f *firehoseMock) DescribeDeliveryStream(*firehose.DescribeDeliveryStreamInput) (*firehose.DescribeDeliveryStreamOutput, error) {
	return nil, nil
}
func (f *firehoseMock) ListDeliveryStreamsRequest(*firehose.ListDeliveryStreamsInput) (*request.Request, *firehose.ListDeliveryStreamsOutput) {
	return nil, nil
}
func (f *firehoseMock) ListDeliveryStreams(*firehose.ListDeliveryStreamsInput) (*firehose.ListDeliveryStreamsOutput, error) {
	return nil, nil
}
func (f *firehoseMock) PutRecordRequest(*firehose.PutRecordInput) (*request.Request, *firehose.PutRecordOutput) {
	return nil, nil
}
func (f *firehoseMock) PutRecord(*firehose.PutRecordInput) (*firehose.PutRecordOutput, error) {
	return nil, nil
}
func (f *firehoseMock) PutRecordBatchRequest(*firehose.PutRecordBatchInput) (*request.Request, *firehose.PutRecordBatchOutput) {
	return nil, nil
}
func (f *firehoseMock) PutRecordBatch(i *firehose.PutRecordBatchInput) (*firehose.PutRecordBatchOutput, error) {
	for _, v := range i.Records {
		var unpacked map[string]string
		_ = json.Unmarshal(v.Data, &unpacked)
		f.received = append(f.received, unpacked)
	}
	return &firehose.PutRecordBatchOutput{}, nil
}
func (f *firehoseMock) UpdateDestinationRequest(*firehose.UpdateDestinationInput) (*request.Request, *firehose.UpdateDestinationOutput) {
	return nil, nil
}
func (f *firehoseMock) UpdateDestination(*firehose.UpdateDestinationInput) (*firehose.UpdateDestinationOutput, error) {
	return nil, nil
}

func TestConfigValidation(t *testing.T) {
	config := KinesisWriterConfig{}
	_ = json.Unmarshal(FirehoseRedshiftStreamTestConfig, &config)
	assert.Nil(t, config.Validate(), "config could not be validated")
}

func TestRedshiftStreamAndCompressValidation(t *testing.T) {
	config := KinesisWriterConfig{}
	_ = json.Unmarshal(FirehoseRedshiftStreamTestConfig, &config)
	config.Compress = true

	// firehose->redshift streaming cannot be used with compress mode
	assert.NotNil(t, config.Validate(), "redshift streaming and compress cannot both be on")
}

func TestRedshiftStreamAndStreamValidation(t *testing.T) {
	config := KinesisWriterConfig{}
	_ = json.Unmarshal(FirehoseRedshiftStreamTestConfig, &config)
	config.StreamType = "stream"

	// firehose->redshift streaming can only be used with firehose
	assert.NotNil(t, config.Validate(), "redshift streaming can only be used with firehose")
}

func TestRedshiftStreamMode(t *testing.T) {
	inputMaps := []map[string]string{
		map[string]string{
			"country":   "US",
			"device_id": "xyz123",
		},
		map[string]string{
			"country":   "",
			"device_id": "",
		},
		map[string]string{
			"country":   "CA",
			"device_id": "\x00",
		},
		map[string]string{
			"country":   "CA",
			"device_id": "xyz\x00123",
		},
		map[string]string{
			"country":   "CA",
			"device_id": "\x00\x00\x00\x00\x00",
		},
	}

	expectedMaps := []map[string]string{
		map[string]string{
			"country":   "US",
			"device_id": "xyz123",
		},
		map[string]string{
			"country":   "",
			"device_id": "",
		},
		map[string]string{
			"country":   "CA",
			"device_id": "",
		},
		map[string]string{
			"country":   "CA",
			"device_id": "xyz123",
		},
		map[string]string{
			"country":   "CA",
			"device_id": "",
		},
	}

	config := KinesisWriterConfig{}
	_ = json.Unmarshal(FirehoseRedshiftStreamTestConfig, &config)

	// create mock objects
	mockInnerStatter := statterMock{}
	mockStatter := &Statter{
		statter:   mockInnerStatter,
		statNames: map[int]string{},
	}
	mockFirehose := firehoseMock{}
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
			assert.Equal(t, mockFirehose.received[i][k], expectedMaps[i][k],
				"output was not equal for input %v key %s", i, k)
		}
	}
}
