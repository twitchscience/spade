package table_config

import (
	"encoding/json"
	"io"
	"io/ioutil"
	"log"
	"os"

	"github.com/twitchscience/scoop_protocol/schema"
	"github.com/twitchscience/spade/transformer"
)

type Tables struct {
	Configs []schema.Event
}

func getTypes(definitions []schema.ColumnDefinition) ([]transformer.RedshiftType, error) {
	types := make([]transformer.RedshiftType, len(definitions))
	for i, definition := range definitions {
		t := transformer.GetTransform(definition.Transformer)
		if t == nil {
			log.Printf("Critical error while parsing config %v\n", transformer.UnknownTransformError)
			return nil, transformer.UnknownTransformError
		}
		_type := transformer.RedshiftType{
			Transformer:  t,
			InboundName:  definition.InboundName,
			OutboundName: definition.OutboundName,
		}
		types[i] = _type
	}
	return types, nil
}

func LoadConfigFromFile(filename string) (Tables, error) {
	file, err := os.Open(filename)
	if err != nil {
		return Tables{nil}, err
	}
	defer file.Close()
	return LoadConfig(file)
}

func LoadConfig(file io.Reader) (Tables, error) {
	b, err := ioutil.ReadAll(file)
	if err != nil {
		return Tables{nil}, err
	}
	var cfgs []schema.Event
	err = json.Unmarshal(b, &cfgs)
	if err != nil {
		return Tables{nil}, err
	}
	return Tables{cfgs}, nil
}

func (c *Tables) CompileForParsing() (map[string][]transformer.RedshiftType, map[string]int, error) {
	configs := make(map[string][]transformer.RedshiftType)
	versions := make(map[string]int)
	for _, config := range c.Configs {
		typedConfig, typeErr := getTypes(config.Columns)
		if typeErr == nil {
			configs[config.EventName] = typedConfig
			versions[config.EventName] = config.Version
		}
	}
	return configs, versions, nil
}

func (c *Tables) CompileForMaintenance() map[string]schema.Event {
	creationStrings := make(map[string]schema.Event)
	for _, config := range c.Configs {
		creationStrings[config.EventName] = config
	}
	return creationStrings
}
