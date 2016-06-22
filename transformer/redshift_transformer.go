package transformer

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/twitchscience/spade/parser"
	"github.com/twitchscience/spade/reporter"
	"github.com/twitchscience/spade/writer"
)

type RedshiftTransformer struct {
	Configs ConfigLoader
}

type nontrackedEvent struct {
	Event      string          `json:"event"`
	Properties json.RawMessage `json:"properties"`
}

func NewRedshiftTransformer(configs ConfigLoader) Transformer {
	return &RedshiftTransformer{
		Configs: configs,
	}
}

// MixpanelEvent -> WriteRequest
func (t *RedshiftTransformer) Consume(event *parser.MixpanelEvent) *writer.WriteRequest {
	version := t.Configs.GetVersionForEvent(event.Event)

	if event.Failure != reporter.NONE {
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
			Failure:  reporter.NONE,
			Pstart:   event.Pstart,
		}
	}
	switch err.(type) {
	case TransformError:
		return &writer.WriteRequest{
			Category: event.Event,
			Version:  version,
			Line:     err.Error(),
			UUID:     event.UUID,
			Source:   event.Properties,
			Failure:  reporter.UNABLE_TO_PARSE_DATA,
			Pstart:   event.Pstart,
		}
	case NotTrackedError:
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
			Failure:  reporter.NON_TRACKING_EVENT,
			Pstart:   event.Pstart,
		}
	case SkippedColumnError: // Non critical error
		return &writer.WriteRequest{
			Category: event.Event,
			Version:  version,
			Line:     line,
			Record:   kv,
			UUID:     event.UUID,
			Source:   event.Properties,
			Failure:  reporter.SKIPPED_COLUMN,
			Pstart:   event.Pstart,
		}
	default:
		return &writer.WriteRequest{
			Category: "Unknown",
			Version:  version,
			Line:     "",
			UUID:     event.UUID,
			Source:   event.Properties,
			Failure:  reporter.EMPTY_REQUEST,
			Pstart:   event.Pstart,
		}
	}
}

func (t *RedshiftTransformer) transform(event *parser.MixpanelEvent) (string, map[string]string, error) {
	if event.Event == "" {
		return "", nil, EmptyRequestError{fmt.Sprintf("%v is not being tracked", event.Event)}
	}

	var possibleError error = nil
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
	decoder.Decode(&temp)
	// Always replace the timestamp with server Time
	if _, ok := temp["time"]; ok {
		temp["client_time"] = temp["time"]
	}
	temp["time"] = event.EventTime

	// Still allow clients to override the ip address.
	if _, ok := temp["ip"]; !ok {
		temp["ip"] = event.ClientIp
	}

	for n, column := range columns {
		k, v, err := column.Format(temp)
		// We should add reporting here to statsd
		if err != nil {
			possibleError = SkippedColumnError{fmt.Sprintf("Problem parsing into %v: %v\n", column, err)}
		}
		if n != 0 {
			tsvOutput.WriteRune('\t')
		}
		tsvOutput.WriteString(fmt.Sprintf("%q", v))
		kvOutput[k] = v
	}
	return tsvOutput.String(), kvOutput, possibleError
}
