package table_config

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/twitchscience/scoop_protocol/scoop_protocol"
)

func newColumnDefs(in, out, transformer, opts string) scoop_protocol.ColumnDefinition {
	return scoop_protocol.ColumnDefinition{
		InboundName:           in,
		OutboundName:          out,
		Transformer:           transformer,
		ColumnCreationOptions: opts,
	}
}

func buildConfig() []byte {
	data := []scoop_protocol.Config{
		{
			EventName: "test1",
			Columns: []scoop_protocol.ColumnDefinition{
				newColumnDefs("testIn", "test", "int", ""),
				newColumnDefs("testCharIn", "testChar", "varchar", "(32)"),
				newColumnDefs("testIn", "test", "f@timestamp@2006-01-02 15:04:05", ""),
			},
			Version: 22,
		},
		{
			EventName: "test2",
			Columns: []scoop_protocol.ColumnDefinition{
				newColumnDefs("testbIn", "testb", "int", ""),
				newColumnDefs("testCharbIn", "testCharb", "varchar", "(32)"),
			},
			Version: 4,
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
	maintenanceStrings, maintananceVersions, _ := tables.CompileForParsing()
	loader := NewStaticLoader(maintenanceStrings, maintananceVersions)
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
	_, err = loader.GetColumnsForEvent("DoesNotExist")
	if err == nil {
		t.Fatalf("expected to not have DoesNotExist\n")
		t.FailNow()
	}
}

func TestVersionLoading(t *testing.T) {
	tables, err := LoadConfig(bytes.NewReader(buildConfig()))
	if err != nil {
		t.Error(err)
		t.Fail()
	}
	maintenanceStrings, maintananceVersions, _ := tables.CompileForParsing()
	loader := NewStaticLoader(maintenanceStrings, maintananceVersions)
	version := loader.GetVersionForEvent("test1")
	if version != 22 {
		t.Fatalf("expected to have version == 22\n")
		t.FailNow()
	}
	version = loader.GetVersionForEvent("test2")
	if version != 4 {
		t.Fatalf("expected to have version == 4\n")
		t.FailNow()
	}

	version = loader.GetVersionForEvent("DoesNotExist")
	if version != 0 {
		t.Fatalf("expected to have version == 0\n")
		t.FailNow()
	}
}
