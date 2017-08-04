package tables

import (
	"encoding/json"
	"io"
	"io/ioutil"
	"os"
	"strings"

	"github.com/twitchscience/aws_utils/logger"
	"github.com/twitchscience/scoop_protocol/scoop_protocol"
	"github.com/twitchscience/spade/geoip"
	"github.com/twitchscience/spade/transformer"
)

// Tables is a list of versioned events and their definitions.
type Tables struct {
	Configs []scoop_protocol.Config
}

func getTypes(
	definitions []scoop_protocol.ColumnDefinition,
	tConfigs map[string]transformer.MappingTransformerConfig,
	geoip geoip.GeoLookup,
) ([]transformer.RedshiftType, error) {
	types := make([]transformer.RedshiftType, len(definitions))
	for i, definition := range definitions {
		var t transformer.ColumnTransformer
		var supportingColumns []string
		tConfig, ok := tConfigs[definition.Transformer]
		if ok {
			t = transformer.GetMappingTransform(definition.Transformer, tConfig)
			supportingColumns = strings.Split(definition.SupportingColumns, ",")
		} else {
			t = transformer.GetSingleValueTransform(definition.Transformer, geoip)
		}
		if t == nil {
			logger.WithError(transformer.ErrUnknownTransform).WithField(
				"transform", definition.Transformer).Error("Failed to parse config")
			return nil, transformer.ErrUnknownTransform
		}
		_type := transformer.RedshiftType{
			Transformer:       t,
			InboundName:       definition.InboundName,
			OutboundName:      definition.OutboundName,
			SupportingColumns: supportingColumns,
		}
		types[i] = _type
	}
	return types, nil
}

// LoadConfigFromFile loads a Tables from the given filename.
func LoadConfigFromFile(filename string) (Tables, error) {
	file, err := os.Open(filename)
	if err != nil {
		return Tables{nil}, err
	}
	defer func() {
		if err := file.Close(); err != nil {
			logger.WithError(err).WithField("filename", filename).Error(
				"Failed to close table config")
		}
	}()
	return LoadConfig(file)
}

// LoadConfig loads a Tables from the given io.Reader.
func LoadConfig(file io.Reader) (Tables, error) {
	b, err := ioutil.ReadAll(file)
	if err != nil {
		return Tables{nil}, err
	}
	var cfgs []scoop_protocol.Config
	err = json.Unmarshal(b, &cfgs)
	if err != nil {
		return Tables{nil}, err
	}
	return Tables{cfgs}, nil
}

// CompileForParsing returns a map of transformers and versions for our table configs.
func (c *Tables) CompileForParsing(
	tConfigs map[string]transformer.MappingTransformerConfig,
	geoip geoip.GeoLookup,
) (map[string][]transformer.RedshiftType, map[string]int, error) {
	configs := make(map[string][]transformer.RedshiftType)
	versions := make(map[string]int)
	for _, config := range c.Configs {
		typedConfig, typeErr := getTypes(config.Columns, tConfigs, geoip)
		if typeErr == nil {
			configs[config.EventName] = typedConfig
			versions[config.EventName] = config.Version
		}
	}
	return configs, versions, nil
}

// CompileForMaintenance turns our list of Configs into a map.
func (c *Tables) CompileForMaintenance() map[string]scoop_protocol.Config {
	creationStrings := make(map[string]scoop_protocol.Config)
	for _, config := range c.Configs {
		creationStrings[config.EventName] = config
	}
	return creationStrings
}
