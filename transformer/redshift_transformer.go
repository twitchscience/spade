package transformer

import (
	"bytes"
	"encoding/json"
	"fmt"
	"time"

	"github.com/twitchscience/aws_utils/logger"
	"github.com/twitchscience/scoop_protocol/scoop_protocol"
	"github.com/twitchscience/scoop_protocol/spade"
	"github.com/twitchscience/spade/parser"
	"github.com/twitchscience/spade/reporter"
	"github.com/twitchscience/spade/writer"

	"github.com/twitchscience/spade/lookup"
)

// RedshiftTransformer turns MixpanelEvents into WriteRequests using the given SchemaConfigLoader.
type RedshiftTransformer struct {
	Configs              SchemaConfigLoader
	EventMetadataConfigs EventMetadataConfigLoader
	stats                reporter.StatsLogger
}

type nontrackedEvent struct {
	Event      string          `json:"event"`
	Properties json.RawMessage `json:"properties"`
}

// NewRedshiftTransformer creates a new RedshiftTransformer using the given SchemaConfigLoader and EventMetadataConfigLoader
func NewRedshiftTransformer(configs SchemaConfigLoader, eventMetadataConfigs EventMetadataConfigLoader, stats reporter.StatsLogger) Transformer {
	// func NewRedshiftTransformer(configs SchemaConfigLoader, stats reporter.StatsLogger) Transformer {
	return &RedshiftTransformer{
		Configs:              configs,
		EventMetadataConfigs: eventMetadataConfigs,
		stats:                stats,
	}
}

// Consume transforms a MixpanelEvent into a WriteRequest.
func (t *RedshiftTransformer) Consume(event *parser.MixpanelEvent) *writer.WriteRequest {
	version := t.Configs.GetVersionForEvent(event.Event)

	if event.Failure != reporter.None {
		return &writer.WriteRequest{
			Category: event.Event,
			Version:  version,
			Line:     "",
			UUID:     event.UUID,
			Source:   event.Properties,
			Failure:  event.Failure,
			Pstart:   event.Pstart,
		}
	}

	t1 := time.Now()
	line, kv, err := t.transform(event)
	t.stats.Timing(fmt.Sprintf("transformer.%s", event.Event), time.Since(t1)/time.Millisecond)

	if err == nil {
		return &writer.WriteRequest{
			Category: event.Event,
			Version:  version,
			Line:     line,
			Record:   kv,
			UUID:     event.UUID,
			Source:   event.Properties,
			Failure:  reporter.None,
			Pstart:   event.Pstart,
		}
	}
	switch err.(type) {
	case ErrNotTracked:
		dump, err := json.Marshal(&nontrackedEvent{
			Event:      event.Event,
			Properties: event.Properties,
		})
		if err != nil {
			dump = []byte("")
		}
		return &writer.WriteRequest{
			Category: event.Event,
			Version:  version,
			Line:     string(dump),
			UUID:     event.UUID,
			Source:   event.Properties,
			Failure:  reporter.NonTrackingEvent,
			Pstart:   event.Pstart,
		}
	case ErrSkippedColumn: // Non critical error
		return &writer.WriteRequest{
			Category: event.Event,
			Version:  version,
			Line:     line,
			Record:   kv,
			UUID:     event.UUID,
			Source:   event.Properties,
			Failure:  reporter.SkippedColumn,
			Pstart:   event.Pstart,
		}
	default:
		return &writer.WriteRequest{
			Category: "Unknown",
			Version:  version,
			Line:     "",
			UUID:     event.UUID,
			Source:   event.Properties,
			Failure:  reporter.EmptyRequest,
			Pstart:   event.Pstart,
		}
	}
}

func (t *RedshiftTransformer) transform(event *parser.MixpanelEvent) (string, map[string]string, error) {
	if event.Event == "" {
		return "", nil, ErrEmptyRequest
	}

	var possibleError error
	columns, err := t.Configs.GetColumnsForEvent(event.Event)
	if err != nil {
		return "", nil, err
	}

	var tsvOutput bytes.Buffer
	kvOutput := make(map[string]string)

	// We can probably make this so that it never actually needs to decode the json
	// If each table knew which byte sequences a column corresponds to we can
	// dynamically build a state machine to scrape each column from the raw byte array
	temp := make(map[string]interface{}, 15)
	decoder := json.NewDecoder(bytes.NewReader(event.Properties))
	decoder.UseNumber()
	if err = decoder.Decode(&temp); err != nil {
		return "", nil, err
	}

	// CURRENT TASK: get expected edge type from Blueprint for event, then
	// Actually check it here
	expectedEdgeType, err := t.EventMetadataConfigs.GetMetadataValueByType(event.Event, string(scoop_protocol.EDGE_TYPE))
	if err != nil {
		return "", nil, err
	}

	logger.Error(fmt.Sprintf("Transform: event: %s, Actual Edge: %s, Expected Edge: %s", event.Event, event.EdgeType, expectedEdgeType))
	if expectedEdgeType != event.EdgeType {
		// logger.Error(sprintf("Edge Type Mismatch: "))
		logger.Error(fmt.Sprintf("+1 on edge-type-mismatch.%s.%s.%s", event.Event, event.EdgeType, expectedEdgeType))
		t.stats.IncrBy(fmt.Sprintf("edge-type-mismatch.%s.%s.%s", event.Event, event.EdgeType, expectedEdgeType), 1)
	}

	if event.EdgeType == spade.INTERNAL_EDGE || event.EdgeType == spade.EXTERNAL_EDGE {
		t.stats.IncrBy(fmt.Sprintf("edge-type.%s.%s", event.Event, event.EdgeType), 1)
	} else {
		logger.WithField("edgeType", event.EdgeType).Error("Invalid edge type")
	}

	// Always replace the timestamp with server Time
	if _, ok := temp["time"]; ok {
		temp["client_time"] = temp["time"]
	}
	temp["time"] = event.EventTime

	// Still allow clients to override the ip address.
	if _, ok := temp["ip"]; !ok {
		temp["ip"] = event.ClientIP
	}

	// Still allow clients to override the user agent.
	if _, ok := temp["user_agent"]; !ok && event.UserAgent != "" {
		temp["user_agent"] = event.UserAgent
	}

	results := make(map[string]int)
	for n, column := range columns {
		k, v, err := column.Format(temp)
		skipped := false
		switch err {
		case nil:
			results["success"]++
		case lookup.ErrTooManyRequests:
			skipped = true
			results["tooManyFetchRequests"]++
		case lookup.ErrExtractingValue:
			skipped = true
			results["invalidMapping"]++
		case ErrIDSet:
			results["success"]++
			results["cache.id_set"]++
		case ErrBadLookupValue:
			skipped = true
			results["cache.bad_lookup_value"]++
		case ErrEmptyLookupValue:
			skipped = true
			results["cache.empty_lookup_value"]++
		case ErrLocalCacheHit:
			results["success"]++
			results["cache.local_cache_hit"]++
		case ErrRemoteCacheHit:
			results["success"]++
			results["cache.remote_cache_hit"]++
		case ErrFetchSuccess:
			results["success"]++
			results["cache.fetch_success"]++
		case ErrFetchFailure:
			skipped = true
			results["cache.fetch_failure"]++
		case ErrCacheSetFailure:
			results["success"]++
			results["cache.set_failure"]++
		default:
			skipped = true
		}
		if skipped {
			results["skippedColumn"]++
			possibleError = ErrSkippedColumn{
				fmt.Sprintf("Problem parsing into %v: %v\n", column, err),
			}
		}
		if n != 0 {
			_, _ = tsvOutput.WriteRune('\t')
		}
		_, _ = tsvOutput.WriteString(fmt.Sprintf("%q", v))
		kvOutput[k] = v
	}
	for stat, count := range results {
		t.stats.IncrBy(fmt.Sprintf("transformer.%s.%s", event.Event, stat), count)
	}

	return tsvOutput.String(), kvOutput, possibleError
}
