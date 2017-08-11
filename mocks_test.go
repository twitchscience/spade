package main

import (
	"bytes"
	"compress/flate"
	"compress/gzip"
	"encoding/base64"
	"encoding/json"
	"io/ioutil"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/elasticache"
	"github.com/aws/aws-sdk-go/service/elasticache/elasticacheiface"
	"github.com/aws/aws-sdk-go/service/firehose"
	"github.com/aws/aws-sdk-go/service/firehose/firehoseiface"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/aws/aws-sdk-go/service/s3/s3manager/s3manageriface"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/aws/aws-sdk-go/service/sns/snsiface"
	"github.com/bradfitz/gomemcache/memcache"
	"github.com/twitchscience/scoop_protocol/spade"
	"github.com/twitchscience/spade/cache/elastimemcache"
	"github.com/twitchscience/spade/consumer"
	"github.com/twitchscience/spade/geoip"
	"github.com/twitchscience/spade/lookup"
	"github.com/twitchscience/spade/reporter"
)

type sentRecord struct {
	source string
	data   string
}

// s3Mock returns the fetcherConfigs defined in main_test.go from the ConfigBucket bucket.
type s3Mock struct {
	sentRecords    chan sentRecord
	fetcherConfigs map[string][]byte
	s3iface.S3API
}

func newS3Mock(sentRecords chan sentRecord) (*s3Mock, error) {
	m := &s3Mock{
		sentRecords:    sentRecords,
		fetcherConfigs: make(map[string][]byte),
	}
	for key, val := range fetcherConfigs {
		buf := &bytes.Buffer{}
		w := gzip.NewWriter(buf)
		_, err := w.Write([]byte(val))
		if err != nil {
			return nil, err
		}
		err = w.Close()
		if err != nil {
			return nil, err
		}
		m.fetcherConfigs[key] = buf.Bytes()
	}
	return m, nil
}

func (m *s3Mock) GetObject(input *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	if aws.StringValue(input.Bucket) == "ConfigBucket" {
		output := m.fetcherConfigs[aws.StringValue(input.Key)]
		return &s3.GetObjectOutput{
			Body: ioutil.NopCloser(bytes.NewBuffer(output)),
		}, nil
	}
	return nil, nil
}

// s3UploaderMock captures all files uploaded and sends them along with the bucket name
// to sentRecords.
type s3UploaderMock struct {
	sentRecords chan sentRecord
	s3manageriface.UploaderAPI
}

func (m *s3UploaderMock) Upload(input *s3manager.UploadInput, options ...func(*s3manager.Uploader)) (
	*s3manager.UploadOutput, error) {
	reader, err := gzip.NewReader(input.Body)
	if err != nil {
		return nil, err
	}
	body, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	m.sentRecords <- sentRecord{
		source: aws.StringValue(input.Bucket),
		data:   string(body),
	}
	return nil, nil
}

// snsMock forwards any received messages to sentRecords with the TopicArn.
type snsMock struct {
	sentRecords chan sentRecord
	snsiface.SNSAPI
}

func (m *snsMock) Publish(input *sns.PublishInput) (*sns.PublishOutput, error) {
	m.sentRecords <- sentRecord{
		source: aws.StringValue(input.TopicArn),
		data:   aws.StringValue(input.Message),
	}
	return nil, nil
}

// elasticacheMock returns a dummy elasticache endpoint.
type elasticacheMock struct {
	elasticacheiface.ElastiCacheAPI
}

func (m *elasticacheMock) DescribeCacheClusters(input *elasticache.DescribeCacheClustersInput) (
	*elasticache.DescribeCacheClustersOutput, error) {
	if *input.CacheClusterId == "eccluster" && *input.ShowCacheNodeInfo {
		return &elasticache.DescribeCacheClustersOutput{
			CacheClusters: []*elasticache.CacheCluster{
				{
					CacheNodes: []*elasticache.CacheNode{
						{
							Endpoint: &elasticache.Endpoint{
								Address: aws.String("sdf"),
								Port:    aws.Int64(123),
							},
						},
					},
				},
			},
		}, nil
	}
	return nil, nil
}

// mockKinesis writes all records received to sentRecords along with the stream name.
type mockKinesis struct {
	sentRecords chan sentRecord
	kinesisiface.KinesisAPI
}

func (m *mockKinesis) PutRecords(input *kinesis.PutRecordsInput) (
	*kinesis.PutRecordsOutput, error) {
	streamName := aws.StringValue(input.StreamName)
	res := kinesis.PutRecordsOutput{
		Records: make([]*kinesis.PutRecordsResultEntry, len(input.Records)),
	}
	for i, record := range input.Records {
		m.sentRecords <- sentRecord{streamName, string(record.Data)}
		res.Records[i] = &kinesis.PutRecordsResultEntry{
			ErrorCode: aws.String(""),
		}
	}
	return &res, nil
}

type kinesisFactory struct {
	sentRecords chan sentRecord
}

func (f *kinesisFactory) New(region, role string) kinesisiface.KinesisAPI {
	if region == "us-west-2" && role == "" {
		return &mockKinesis{sentRecords: f.sentRecords}
	}
	return nil
}

// mockFirehose writes all records received to sentRecords along with the stream name.
type mockFirehose struct {
	sentRecords chan sentRecord
	firehoseiface.FirehoseAPI
}

func (m *mockFirehose) PutRecordBatch(input *firehose.PutRecordBatchInput) (
	*firehose.PutRecordBatchOutput, error) {
	streamName := aws.StringValue(input.DeliveryStreamName)
	res := firehose.PutRecordBatchOutput{
		RequestResponses: make([]*firehose.PutRecordBatchResponseEntry, len(input.Records)),
	}
	for i, record := range input.Records {
		m.sentRecords <- sentRecord{streamName, string(record.Data)}
		res.RequestResponses[i] = &firehose.PutRecordBatchResponseEntry{
			ErrorCode: aws.String(""),
		}
	}
	return &res, nil
}

type firehoseFactory struct {
	sentRecords chan sentRecord
}

func (f *firehoseFactory) New(region, role string) firehoseiface.FirehoseAPI {
	if region == "us-east-1" && role == "iam:role:spade-test-2" {
		return &mockFirehose{sentRecords: f.sentRecords}
	}
	return nil
}

// loginFetcher returns user IDs from fetcherUserIDs in main_test.go.
type loginFetcher struct{}

func (l *loginFetcher) FetchInt64(args map[string]string) (int64, error) {
	return fetcherUserIDs[args["login"]], nil
}

func valueFetcherFactory(l lookup.JSONValueFetcherConfig, s reporter.StatsLogger) (lookup.ValueFetcher, error) {
	return &loginFetcher{}, nil
}

// resultPipeMock encodes records and returns them on its ReadChannel().
type resultPipeMock struct {
	c chan *consumer.Result
}

func (m *resultPipeMock) ReadChannel() <-chan *consumer.Result {
	return m.c
}

func (m *resultPipeMock) Close() {}

func (m *resultPipeMock) WriteBatch(batch []testParam) error {
	transBatch := make([]spade.Event, len(batch))
	for i, param := range batch {
		transBatch[i] = param.event
		// This leaves the extra whitespace in the event, but ehn.
		transBatch[i].Data = base64.StdEncoding.EncodeToString([]byte(param.event.Data))
	}
	encData, err := json.Marshal(transBatch)
	if err != nil {
		return err
	}
	data := bytes.Buffer{}
	data.WriteByte(1)
	compressor, err := flate.NewWriter(&data, flate.BestSpeed)
	if err != nil {
		return err
	}
	if _, err = compressor.Write(encData); err != nil {
		return err
	}

	if err = compressor.Close(); err != nil {
		return err
	}

	m.c <- &consumer.Result{
		Data:  data.Bytes(),
		Error: nil,
	}
	return nil
}

// geoipMock returns a dummy location.
type geoipMock struct {
	geoip.GeoLookup
}

func (m *geoipMock) GetRegion(ip string) string  { return "CA" }
func (m *geoipMock) GetCountry(ip string) string { return "US" }
func (m *geoipMock) GetCity(ip string) string    { return "SF" }
func (m *geoipMock) GetAsn(ip string) string     { return "ASN" }
func (m *geoipMock) Reload() error               { return nil }

// memcacheMock returns values from cachedUserIDs in main_test.go.
type memcacheMock struct {
	elastimemcache.MemcacheClient
}

func (m *memcacheMock) Get(key string) (*memcache.Item, error) {
	if id, ok := cachedUserIDs[key]; ok {
		return &memcache.Item{Value: []byte(id)}, nil
	}
	return nil, memcache.ErrCacheMiss
}

func (m *memcacheMock) Set(item *memcache.Item) error {
	return nil
}

// selectorMock saves the servers called with SetServers for later comparison.
type selectorMock struct {
	elastimemcache.ServerList
	servers []string
}

func (m *selectorMock) SetServers(servers ...string) error {
	m.servers = make([]string, len(servers))
	copy(m.servers, servers)
	return nil
}
