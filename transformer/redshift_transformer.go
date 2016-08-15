package transformer

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/twitchscience/spade/parser"
	"github.com/twitchscience/spade/reporter"
	"github.com/twitchscience/spade/writer"
)

// RedshiftTransformer turns MixpanelEvents into WriteRequests using the given ConfigLoader.
type RedshiftTransformer struct {
	Configs ConfigLoader
}

type nontrackedEvent struct {
	Event      string          `json:"event"`
	Properties json.RawMessage `json:"properties"`
}

// NewRedshiftTransformer creates a new RedshiftTransformer using the given ConfigLoader.
func NewRedshiftTransformer(configs ConfigLoader) Transformer {
	return &RedshiftTransformer{
		Configs: configs,
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

	line, kv, err := t.transform(event)
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
	if err := decoder.Decode(&temp); err != nil {
		return "", nil, err
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

	for n, column := range columns {
		k, v, err := column.Format(temp)
		// We should add reporting here to statsd
		if err != nil {
			possibleError = ErrSkippedColumn{fmt.Sprintf("Problem parsing into %v: %v\n", column, err)}
		}
		if n != 0 {
			_, _ = tsvOutput.WriteRune('\t')
		}
		_, _ = tsvOutput.WriteString(fmt.Sprintf("%q", v))
		kvOutput[k] = v
	}
	return tsvOutput.String(), kvOutput, possibleError
}
