package table_config

import (
	"encoding/json"
	"io"
	"io/ioutil"
	"log"
	"os"

	"github.com/TwitchScience/scoop_protocol/scoop_protocol"
	"github.com/TwitchScience/spade/transformer"
)

type Tables struct {
	Configs []scoop_protocol.Config
}

func getTypes(definitions []scoop_protocol.ColumnDefinition) ([]transformer.RedshiftType, error) {
	types := make([]transformer.RedshiftType, len(definitions))
	for i, definition := range definitions {
		t := transformer.GetTransform(definition.Transformer)
		if t == nil {
			log.Printf("Critical error while parsing config %v\n", transformer.UnknownTransformError)
			return nil, transformer.UnknownTransformError
		}
		_type := transformer.RedshiftType{t, definition.InboundName}
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
	var cfgs []scoop_protocol.Config
	err = json.Unmarshal(b, &cfgs)
	if err != nil {
		return Tables{nil}, err
	}
	return Tables{cfgs}, nil
}

func (c *Tables) CompileForParsing() (map[string][]transformer.RedshiftType, error) {
	configs := make(map[string][]transformer.RedshiftType)
	for _, config := range c.Configs {
		typedConfig, typeErr := getTypes(config.Columns)
		if typeErr == nil {
			configs[config.EventName] = typedConfig
		}
	}
	return configs, nil
}

func (c *Tables) CompileForMaintenance() map[string]scoop_protocol.Config {
	creationStrings := make(map[string]scoop_protocol.Config)
	for _, config := range c.Configs {
		creationStrings[config.EventName] = config
	}
	return creationStrings
}
