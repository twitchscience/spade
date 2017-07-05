package transformer

import (
	"github.com/twitchscience/spade/parser"
	"github.com/twitchscience/spade/writer"
)

// Transformer converts a MixpanelEvent into a WriteRequest.
type Transformer interface {
	Consume(*parser.MixpanelEvent) *writer.WriteRequest
}

// SchemaConfigLoader returns columns (transformers) or versions for given event types.
type SchemaConfigLoader interface {
	GetColumnsForEvent(string) ([]RedshiftType, error)
	GetVersionForEvent(string) int
}

// EventMetadataCOnfigLoader returns event metadata for a given event and metadata type
type EventMetadataConfigLoader interface {
	GetMetadataValueByType(string, string) (string, error)
}
