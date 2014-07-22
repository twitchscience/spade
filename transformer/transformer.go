package transformer

import (
	"github.com/twitchscience/spade/parser"
	"github.com/twitchscience/spade/writer"
)

type Transformer interface {
	Consume(*parser.MixpanelEvent) *writer.WriteRequest
}

type ConfigLoader interface {
	GetColumnsForEvent(string) ([]RedshiftType, error)
}
