package table_config

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/twitchscience/scoop_protocol/scoop_protocol"
)

func makeColumnDefs(in, out, transformer, opts string) scoop_protocol.ColumnDefinition {
	return scoop_protocol.ColumnDefinition{
		InboundName:           in,
		OutboundName:          out,
		Transformer:           transformer,
		ColumnCreationOptions: opts,
	}
}

func buildConfig() []byte {
	data := []scoop_protocol.Config{
		scoop_protocol.Config{
			EventName: "test1",
			Columns: []scoop_protocol.ColumnDefinition{
				makeColumnDefs("testIn", "test", "int", ""),
				makeColumnDefs("testCharIn", "testChar", "varchar", "(32)"),
				makeColumnDefs("testIn", "test", "f@timestamp@2006-01-02 15:04:05", ""),
			},
		},
		scoop_protocol.Config{
			EventName: "test2",
			Columns: []scoop_protocol.ColumnDefinition{
				makeColumnDefs("testbIn", "testb", "int", ""),
				makeColumnDefs("testCharbIn", "testCharb", "varchar", "(32)"),
			},
		},
	}
	configBuffer, _ := json.Marshal(data)
	return configBuffer
}

func TestConfigLoading(t *testing.T) {
	tables, err := LoadConfig(bytes.NewReader(buildConfig()))
	if err != nil {
		t.Error(err)
		t.Fail()
	}
	maintenanceStrings, _ := tables.CompileForParsing()
	loader := NewStaticLoader(maintenanceStrings)
	_, err = loader.GetColumnsForEvent("test1")
	if err != nil {
		t.Fatalf("expected to have test1\n")
		t.FailNow()
	}
	_, err = loader.GetColumnsForEvent("test2")
	if err != nil {
		t.Fatalf("expected to have test2\n")
		t.FailNow()
	}
}
