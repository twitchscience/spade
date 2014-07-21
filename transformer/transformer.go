package transformer

import (
	"github.com/TwitchScience/spade/parser"
	"github.com/TwitchScience/spade/writer"
)

type Transformer interface {
	Consume(*parser.MixpanelEvent) *writer.WriteRequest
}

type ConfigLoader interface {
	GetColumnsForEvent(string) ([]RedshiftType, error)
}
