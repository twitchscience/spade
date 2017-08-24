package main

import (
	"bytes"
	"compress/flate"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/cactus/go-statsd-client/statsd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twitchscience/aws_utils/logger"
	"github.com/twitchscience/scoop_protocol/scoop_protocol"
	"github.com/twitchscience/scoop_protocol/spade"
	"github.com/twitchscience/spade/cache/elastimemcache"
	"github.com/twitchscience/spade/config"
	"github.com/twitchscience/spade/consumer"
	"github.com/twitchscience/spade/geoip"
	"github.com/twitchscience/spade/lookup"
	"github.com/twitchscience/spade/writer"
	"github.com/vrischmann/jsonutil"
)

var (
	fetcherConfigs = map[string]string{
		"SchemasKey": `[{
		"EventName": "video-play",
		"Columns": [{
			"InboundName": "time",
			"OutboundName": "time",
			"Transformer": "f@timestamp@unix",
			"ColumnCreationOptions": " sortkey",
			"SupportingColumns": ""
		}, {
			"InboundName": "time",
			"OutboundName": "time_utc",
			"Transformer": "f@timestamp@unix-utc",
			"ColumnCreationOptions": "",
			"SupportingColumns": ""
		}, {
			"InboundName": "ip",
			"OutboundName": "ip",
			"Transformer": "varchar",
			"ColumnCreationOptions": "(15)",
			"SupportingColumns": ""
		}, {
			"InboundName": "ip",
			"OutboundName": "city",
			"Transformer": "ipCity",
			"ColumnCreationOptions": "",
			"SupportingColumns": ""
		}, {
			"InboundName": "ip",
			"OutboundName": "country",
			"Transformer": "ipCountry",
			"ColumnCreationOptions": "",
			"SupportingColumns": ""
		}, {
			"InboundName": "ip",
			"OutboundName": "region",
			"Transformer": "ipRegion",
			"ColumnCreationOptions": "",
			"SupportingColumns": ""
		}, {
			"InboundName": "ip",
			"OutboundName": "asn",
			"Transformer": "ipAsn",
			"ColumnCreationOptions": "",
			"SupportingColumns": ""
		}, {
			"InboundName": "hls_load_attempted",
			"OutboundName": "hls_load_attempted",
			"Transformer": "bool",
			"ColumnCreationOptions": "",
			"SupportingColumns": ""
		}, {
			"InboundName": "bandwidth",
			"OutboundName": "bandwidth",
			"Transformer": "float",
			"ColumnCreationOptions": "",
			"SupportingColumns": ""
		}, {
			"InboundName": "broadcaster_software",
			"OutboundName": "broadcaster_software",
			"Transformer": "varchar",
			"ColumnCreationOptions": "(32)",
			"SupportingColumns": ""
		}, {
			"InboundName": "buffer_empty_count",
			"OutboundName": "buffer_empty_count",
			"Transformer": "bigint",
			"ColumnCreationOptions": "",
			"SupportingColumns": ""
		}, {
			"InboundName": "channel",
			"OutboundName": "channel",
			"Transformer": "varchar",
			"ColumnCreationOptions": "(25)",
			"SupportingColumns": ""
		}, {
			"InboundName": "channel_id",
			"OutboundName": "channel_id",
			"Transformer": "userIDWithMapping",
			"ColumnCreationOptions": "",
			"SupportingColumns": "channel"
		}, {
			"InboundName": "device_id",
			"OutboundName": "device_id",
			"Transformer": "varchar",
			"ColumnCreationOptions": "(32) distkey",
			"SupportingColumns": ""
		}, {
			"InboundName": "hyphenated-col",
			"OutboundName": "hyphenated-col",
			"Transformer": "int",
			"ColumnCreationOptions": "",
			"SupportingColumns": ""
		}],
		"Version": 2,
		"CreatedTS": "2017-07-18T23:20:08.961473Z",
		"TS": "2017-07-18T23:24:15.698326Z",
		"UserName": "legacy",
		"Dropped": false,
		"DropRequested": true,
		"Reason": ""
	}, {
		"EventName": "minute-watched",
		"Columns": [{
			"InboundName": "time",
			"OutboundName": "time",
			"Transformer": "f@timestamp@unix",
			"ColumnCreationOptions": " sortkey",
			"SupportingColumns": ""
		}, {
			"InboundName": "device_id",
			"OutboundName": "device_id",
			"Transformer": "varchar",
			"ColumnCreationOptions": "(32) distkey",
			"SupportingColumns": ""
		}, {
			"InboundName": "game",
			"OutboundName": "game",
			"Transformer": "varchar",
			"ColumnCreationOptions": "(36)",
			"SupportingColumns": ""
		}, {
			"InboundName": "hyphenated-col",
			"OutboundName": "hyphenated-col",
			"Transformer": "bool",
			"ColumnCreationOptions": "",
			"SupportingColumns": ""
		}],
		"Version": 4,
		"CreatedTS": "2017-07-18T23:20:08.961473Z",
		"TS": "2017-07-24T20:15:35.921243Z",
		"UserName": "unknown",
		"Dropped": false,
		"DropRequested": false,
		"Reason": ""
	}]`,
		"KinesisConfigKey": `[{
		"ID": 150,
		"AWSAccount": 1234567,
		"Team": "Test Team 1",
		"Version": 5,
		"Contact": "#test1",
		"Usage": "Test 1",
		"ConsumingLibrary": "Kinsumer",
		"SpadeConfig": {
			"StreamName": "spade-test-1",
			"StreamRole": "",
			"StreamType": "stream",
			"StreamRegion": "us-west-2",
			"Compress": true,
			"FirehoseRedshiftStream": false,
			"EventNameTargetField": "",
			"ExcludeEmptyFields": false,
			"BufferSize": 1024,
			"MaxAttemptsPerRecord": 10,
			"RetryDelay": "1s",
			"Events": {
				"minute-watched": {
					"Filter": "",
					"Fields": ["time", "device_id", "game", "hyphenated-col"],
					"FieldRenames": {}
				},
				"video-play": {
					"Filter": "",
					"Fields": ["time_utc", "channel", "channel_id", "city", "country", "device_id", "hyphenated-col"],
					"FieldRenames": {}
				}
			},
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
		},
		"LastEditedAt": "2017-07-26T22:13:10.437054Z",
		"LastChangedBy": "test1",
		"Dropped": false,
		"DroppedReason": ""
	}, {
		"ID": 11,
		"AWSAccount": 7654321,
		"Team": "Test Team 2",
		"Version": 0,
		"Contact": "#test2",
		"Usage": "Test 2",
		"ConsumingLibrary": "Firehose",
		"SpadeConfig": {
			"StreamName": "spade-test-2",
			"StreamRole": "iam:role:spade-test-2",
			"StreamType": "firehose",
			"StreamRegion": "us-east-1",
			"Compress": true,
			"FirehoseRedshiftStream": false,
			"EventNameTargetField": "",
			"ExcludeEmptyFields": false,
			"BufferSize": 1024,
			"MaxAttemptsPerRecord": 10,
			"RetryDelay": "1s",
			"Events": {
				"minute-watched": {
					"Filter": "",
					"Fields": ["time"],
					"FieldRenames": null
				},
				"video-play": {
					"Filter": "",
					"Fields": ["time"],
					"FieldRenames": null
				}
			},
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
		},
		"LastEditedAt": "2017-05-05T20:50:57.581352Z",
		"LastChangedBy": "test2",
		"Dropped": false,
		"DroppedReason": ""
	}]`,
		"MetadataConfigKey": `{
		"minute-watched": {
			"edge_type": {
				"MetadataValue": "internal",
				"TS": "2017-07-27T01:10:52.096825Z",
				"UserName": "unknown",
				"Version": 4
			}
		}
	}`,
	}
	fetcherUserIDs = map[string]int64{
		"bestchannel": 25,
	}
	cachedUserIDs = map[string]string{
		"emns:worstchannel": "26",
	}
)

func getTestConfig(spadeDir string) *config.Config {
	return &config.Config{
		SpadeDir:                spadeDir,
		ConfigBucket:            "ConfigBucket",
		SchemasKey:              "SchemasKey",
		KinesisConfigKey:        "KinesisConfigKey",
		MetadataConfigKey:       "MetadataConfigKey",
		ProcessorErrorTopicARN:  "ProcessorErrorTopicARN",
		AceTopicARN:             "AceTopicARN",
		AceErrorTopicARN:        "AceErrorTopicARN",
		NonTrackedTopicARN:      "NonTrackedTopicARN",
		NonTrackedErrorTopicARN: "NonTrackedErrorTopicARN",
		AceBucketName:           "AceBucketName",
		NonTrackedBucketName:    "NonTrackedBucketName",
		MaxLogBytes:             100000000,
		MaxLogAgeSecs:           1,
		NontrackedMaxLogAgeSecs: 1,
		Consumer: consumer.Config{
			ApplicationName:     "Consumer.ApplicationName",
			StreamName:          "Consumer.StreamName",
			ThrottleDelay:       "1s",
			ShardCheckFrequency: "10s",
		},
		Geoip: &geoip.Config{
			ConfigBucket: "Geoip.ConfigBucket",
			IPCity: geoip.Keypath{
				Key:  "Geoip.IpCity.Key",
				Path: "Geoip.IpCity.Path",
			},
			IPASN: geoip.Keypath{
				Key:  "Geoip.IpASN.Key",
				Path: "Geoip.IpASN.Path",
			},
			UpdateFrequencyMins: 60,
			JitterSecs:          100,
		},
		RollbarToken:       "RollbarToken",
		RollbarEnvironment: "RollbarEnvironment",

		KinesisOutputs:                          []scoop_protocol.KinesisWriterConfig{},
		KinesisWriterErrorsBeforeThrottling:     5,
		KinesisWriterErrorThrottlePeriodSeconds: 60,
		JSONValueFetchers: map[string]lookup.JSONValueFetcherConfig{
			"UserIDFetcher": {},
		},

		TransformerCacheCluster: elastimemcache.Config{
			ClusterID: "eccluster",
			Namespace: "emns",
		},
		TransformerFetchers: map[string]string{
			"userIDWithMapping": "UserIDFetcher",
		},

		LRULifetimeSeconds: 3600,

		SchemaReloadFrequency:        jsonutil.FromDuration(5 * time.Minute),
		SchemaRetryDelay:             jsonutil.FromDuration(2 * time.Second),
		KinesisConfigReloadFrequency: jsonutil.FromDuration(10 * time.Minute),
		KinesisConfigRetryDelay:      jsonutil.FromDuration(2 * time.Second),
		EventMetadataReloadFrequency: jsonutil.FromDuration(5 * time.Minute),
		EventMetadataRetryDelay:      jsonutil.FromDuration(2 * time.Second),

		StatsdHostport: "StatsdHostport",
		StatsdPrefix:   "StatsdPrefix",
	}
}

func decodeCompressedKinesis(data string) (*writer.Record, []writer.Event, error) {
	var rec writer.Record
	err := json.Unmarshal([]byte(data), &rec)
	if err != nil {
		return nil, nil, err
	}
	buf := bytes.NewBuffer(rec.Data)
	version, err := buf.ReadByte()
	if err != nil {
		return nil, nil, err
	}
	if version != 1 {
		return nil, nil, fmt.Errorf("Unknown compression version: %v", version)
	}
	deflator := flate.NewReader(buf)
	deflated, err := ioutil.ReadAll(deflator)
	if err != nil {
		return nil, nil, err
	}
	var events []writer.Event
	err = json.Unmarshal(deflated, &events)
	if err != nil {
		return nil, nil, err
	}

	return &rec, events, nil
}

// waitForProcessor waits for records to come in on sentRecords and validates them against the
// testParam. It returns a count of how many records of each type it received. It will return
// after 5 seconds or receiving all the records it expects in testP.expectedCount,
// whichever is first.
func waitForProcessor(t *testing.T, sentRecords chan sentRecord, testP testParam, cfg *config.Config) map[string]int {
	timeout := time.After(5 * time.Second)
	recordCount := map[string]int{}
	for {
		select {
		case <-timeout:
			return recordCount
		case record := <-sentRecords:
			t.Logf("Decoding %s: %s", record.source, record.data)
			recordCount[record.source]++
			switch record.source {
			case "spade-test-1":
				rec, events, err := decodeCompressedKinesis(record.data)
				require.NoError(t, err)
				assert.Equal(t, 1, rec.Version)
				require.Equal(t, 1, len(events))
				assert.Equal(t, testP.eventName, events[0].Name)
			case "spade-test-2":
				rec, events, err := decodeCompressedKinesis(record.data)
				require.NoError(t, err)
				assert.Equal(t, 1, rec.Version)
				require.Equal(t, 1, len(events))
				assert.Equal(t, testP.eventName, events[0].Name)
			case "AceBucketName":
				if testP.tsv == nil {
					assert.Equal(t, "", string(record.data))
				} else {
					parts := strings.Split(record.data[1:len(record.data)-2], "\"\t\"")
					assert.Equal(t, testP.tsv, parts)
				}
			case "NonTrackedBucketName":
				var expectedEvent, receivedEvent spade.Event
				err := json.Unmarshal([]byte(testP.event.Data), &expectedEvent)
				require.NoError(t, err)
				err = json.Unmarshal([]byte(record.data), &receivedEvent)
				require.NoError(t, err)
				assert.Equal(t, expectedEvent, receivedEvent)
			case "AceTopicARN":
				var req scoop_protocol.RowCopyRequest
				err := json.Unmarshal([]byte(record.data), &req)
				require.NoError(t, err)
				assert.Equal(t, testP.eventName, req.TableName)
				assert.Equal(t, testP.event.Version, req.TableVersion)
				assert.Regexp(t, cfg.AceBucketName, req.KeyName)
			case "NonTrackedTopicARN":
				var req scoop_protocol.RowCopyRequest
				err := json.Unmarshal([]byte(record.data), &req)
				require.NoError(t, err)
				assert.Equal(t, "", req.TableName)
				assert.Equal(t, 0, req.TableVersion)
				assert.Regexp(t, cfg.NonTrackedBucketName, req.KeyName)
			default:
				t.Errorf("Unknown source: %s", record.source)
			}
			if reflect.DeepEqual(recordCount, testP.expectedCount) {
				return recordCount
			}
		}
	}
}

type testParam struct {
	name          string
	event         spade.Event
	tsv           []string
	eventName     string
	expectedCount map[string]int
	resetSpade    bool
}

var testParams = []testParam{
	{
		name: "normal event",
		event: spade.Event{
			ReceivedAt: time.Date(2017, 8, 1, 18, 7, 35, 51326122, time.UTC),
			ClientIp:   net.IPv4(192, 168, 1, 100),
			Uuid:       "0",
			Data: `{"event": "video-play",
			"properties": {
				"hls_load_attempted": false,
				"hyphenated-col": 6,
				"vod_type": "",
				"vod_id": "",
				"device_id": "7ef35cbd3b4175c1",
				"hls_load_attempted": false,
				"game": "Terraria",
				"broadcaster_software": "xbox360",
				"channel": "bestchannel",
				"bandwidth": 31.933,
				"current_bitrate": -2178,
				"buffer_empty_count": "1",
				"content_mode": "other"}}`,
			Version:  2,
			EdgeType: "internal",
		},
		tsv: []string{"2017-08-01 11:07:35", "2017-08-01 18:07:35", "192.168.1.100",
			"SF", "US", "CA", "ASN", "false", "31.933", "xbox360", "1",
			"bestchannel", "25", "7ef35cbd3b4175c1", "6"},
		expectedCount: map[string]int{
			"spade-test-1":  1,
			"spade-test-2":  1,
			"AceBucketName": 1,
			"AceTopicARN":   1,
		},
		eventName:  "video-play",
		resetSpade: false,
	},
	{
		name: "untracked event",
		event: spade.Event{
			ReceivedAt: time.Date(2017, 8, 1, 18, 7, 35, 51326122, time.UTC),
			ClientIp:   net.IPv4(192, 168, 1, 100),
			Uuid:       "0",
			Data: `{"event": "unknown",
			"properties": {
				"hls_load_attempted": true,
				"hyphenated-col": 2
				}}`,
			Version:  0,
			EdgeType: "internal",
		},
		tsv: nil,
		expectedCount: map[string]int{
			"NonTrackedBucketName": 1,
			"NonTrackedTopicARN":   1,
		},
		eventName:  "unknown",
		resetSpade: true,
	},
}

func TestE2E(t *testing.T) {
	logger.Init("info")
	sentRecords := make(chan sentRecord, 10)
	s3M, err := newS3Mock(sentRecords)
	require.NoError(t, err)
	s3UploaderM := &s3UploaderMock{sentRecords: sentRecords}
	snsM := &snsMock{sentRecords: sentRecords}
	elasticacheM := &elasticacheMock{}

	resultPipeM := &resultPipeMock{make(chan *consumer.Result, 10)}
	memcacheClientM := &memcacheMock{}
	memcacheSelectorM := &selectorMock{}

	deps := spadeProcessorDeps{
		s3:              s3M,
		s3Uploader:      s3UploaderM,
		sns:             snsM,
		elasticache:     elasticacheM,
		kinesisFactory:  &kinesisFactory{sentRecords},
		firehoseFactory: &firehoseFactory{sentRecords},

		valueFetcherFactory: valueFetcherFactory,
		geoip:               &geoipMock{},
		resultPipe:          resultPipeM,
		memcacheClient:      memcacheClientM,
		memcacheSelector:    memcacheSelectorM,
		stats:               &statsd.NoopClient{},
	}
	require.NoError(t, os.Setenv("HOST", "HOST"))
	require.NoError(t, os.Setenv("CLOUD_CLUSTER", "CLUSTER"))
	require.NoError(t, os.Setenv("CLOUD_AUTO_SCALE_GROUP", "ASG"))
	for _, testP := range testParams {
		t.Logf("Testing %s", testP.name)
		spadeDir, err := ioutil.TempDir("", "")
		require.NoError(t, err)
		defer func() {
			require.NoError(t, os.RemoveAll(spadeDir))
		}()
		deps.cfg = getTestConfig(spadeDir)
		processor, err := newProcessor(&deps)
		require.NoError(t, err)
		assert.NotNil(t, processor)
		assert.Equal(t, []string{"sdf:123"}, memcacheSelectorM.servers)

		logger.Go(func() {
			processor.run()
			processor.shutdown()
		})
		require.NoError(t, resultPipeM.WriteBatch([]testParam{testP}))
		if testP.resetSpade {
			// Sleep to give time to read from the result pipe.
			time.Sleep(10 * time.Millisecond)
			processor.sigc <- syscall.SIGINT
		}
		recordCount := waitForProcessor(t, sentRecords, testP, deps.cfg)
		assert.Equal(t, testP.expectedCount, recordCount)
		if !testP.resetSpade {
			processor.sigc <- syscall.SIGINT
		}
		select {
		case record := <-sentRecords:
			t.Errorf("Extra record received for test case %s: %v", testP.name, record)
		default:
		}
		err = filepath.Walk(spadeDir, func(path string, info os.FileInfo, err error) error {
			if info.IsDir() {
				return nil
			}
			return fmt.Errorf("unexpected file: %s", path)
		})
		assert.NoError(t, err)
	}
}
