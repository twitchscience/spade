package transformer

import (
	"bytes"
	"encoding/json"
	"fmt"

	"strings"

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
	if event.Failure != reporter.NONE {
		return &writer.WriteRequest{
			Category: event.Event,
			Line:     "",
			UUID:     event.UUID,
			Source:   event.Properties,
			Failure:  event.Failure,
			Pstart:   event.Pstart,
		}
	}

	line, err := t.transform(event)
	if err == nil {
		return &writer.WriteRequest{
			Category: event.Event,
			Line:     line,
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
			Line:     string(dump),
			UUID:     event.UUID,
			Source:   event.Properties,
			Failure:  reporter.NON_TRACKING_EVENT,
			Pstart:   event.Pstart,
		}
	case SkippedColumnError: // Non critical error
		return &writer.WriteRequest{
			Category: event.Event,
			Line:     line,
			UUID:     event.UUID,
			Source:   event.Properties,
			Failure:  reporter.SKIPPED_COLUMN,
			Pstart:   event.Pstart,
		}
	default:
		return &writer.WriteRequest{
			Category: "Unknown",
			Line:     "",
			UUID:     event.UUID,
			Source:   event.Properties,
			Failure:  reporter.EMPTY_REQUEST,
			Pstart:   event.Pstart,
		}
	}
}

func (t *RedshiftTransformer) transform(event *parser.MixpanelEvent) (string, error) {
	if event.Event == "" {
		return "", EmptyRequestError{fmt.Sprintf("%v is not being tracked", event.Event)}
	}

	var possibleError error = nil
	columns, err := t.Configs.GetColumnsForEvent(event.Event)
	if err != nil {
		return "", err
	}

	output := make([]string, len(columns))

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
		o, err := column.Format(temp)
		// We should add reporting here to statsd
		if err != nil {
			possibleError = SkippedColumnError{fmt.Sprintf("Problem parsing into %v: %v\n", column, err)}
		}
		output[n] = o
	}
	return strings.Join(output, "\t"), possibleError
}
