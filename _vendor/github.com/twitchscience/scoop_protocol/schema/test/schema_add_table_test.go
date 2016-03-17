package test

import (
	"fmt"
	"reflect"
	"testing"

	s "github.com/twitchscience/scoop_protocol/schema"
)

func TestAddTable(t *testing.T) {
	AddTableIsColumnSchemaEmpty(t)
	AddTableHasValidTableOptions(t)
	AddTableIsValidEventIdentifier(t)
	AddTableAddColumnOnly(t)
	AddTableAddColumnNoTransformerJunk(t)
	AddTableAddColumnVarCharLessThan64k(t)
	AddTableAddColumnOutboundNameCollision(t)
	AddTableAddColumnColumnNameLen(t)
	AddTableAddColumnColumnsMoreThan300(t)

	t.Logf("Testing working Add Table now")
	testEvent := SimEvent1Version1()
	testMigration := SimEvent1Migration1()
	migrator := s.NewMigratorBackend(testMigration, testEvent)
	newEvent, err := migrator.ApplyMigration()
	if err != nil {
		t.Errorf("Expected passed migration, instead errored: %s", err)
	} else {
		t.Logf("Passed migration successfully: %+v", newEvent)
	}

	if !reflect.DeepEqual(newEvent, SimEvent1Version2()) {
		t.Errorf("expected: %+v recieved %+v", SimEvent1Version2(), newEvent)
	}
}

func AddTableIsColumnSchemaEmpty(t *testing.T) {
	t.Log("Testing if the Column Schema is already empty when adding table")
	testEvent := EventTest1()
	testMigration := Migration1OnEvent1()
	migrator := s.NewMigratorBackend(testMigration, testEvent)
	newEvent, err := migrator.ApplyMigration()
	if err == nil {
		t.Error("Test should have failed as original schema had existing event")
		t.Logf("%+v", newEvent)
	}

	testEvent = EventTest1Empty()
	migrator = s.NewMigratorBackend(testMigration, testEvent)
	newEvent, err = migrator.ApplyMigration()
	if err != nil {
		t.Errorf("Expected passed migration, instead errored: %s", err)
	} else {
		t.Logf("Passed migration successfully: %+v", newEvent)
	}
}

func AddTableHasValidTableOptions(t *testing.T) {

	t.Log("Testing to see if the migration has valid table options")
	testMigration := Migration1OnEvent1()
	testEvent := EventTest1Empty()
	migrator := s.NewMigratorBackend(testMigration, testEvent)
	newEvent, err := migrator.ApplyMigration()
	if err != nil {
		t.Errorf("Expected passed migration, instead errored: %s", err)
	} else {
		t.Logf("Passed migration successfully: %+v", newEvent)
	}

	testMigration.TableOption.DistKey = []string{"not_passing_tableoption_distkey"}
	migrator = s.NewMigratorBackend(testMigration, testEvent)
	newEvent, err = migrator.ApplyMigration()
	if err == nil {
		t.Error("Test should have failed as migration disykey not contained in any outbound col names")
		t.Logf("%+v", newEvent)
	}

	testMigration.TableOption.DistKey = []string{}
	migrator = s.NewMigratorBackend(testMigration, testEvent)
	newEvent, err = migrator.ApplyMigration()
	if err == nil {
		t.Error("Test should have failed as migration distkey does not contain any values")
		t.Logf("%+v", newEvent)
	}
}

func AddTableIsValidEventIdentifier(t *testing.T) {
	t.Log("Testing to see if the event name is a valid identifier")
	testMigration := Migration1OnEvent1()
	testEvent := EventTest1Empty()
	migrator := s.NewMigratorBackend(testMigration, testEvent)
	newEvent, err := migrator.ApplyMigration()
	if err != nil {
		t.Errorf("Expected passed migration, instead errored: %s", err)
	} else {
		t.Logf("Passed migration successfully: %+v", newEvent)
	}

	testMigration.Name = ""
	testEvent.EventName = ""
	migrator = s.NewMigratorBackend(testMigration, testEvent)
	newEvent, err = migrator.ApplyMigration()
	if err == nil {
		t.Error("Test should have failed as Event Name is less than 1 char")
		t.Logf("%+v", newEvent)
	}

	testMigration.Name = "abcdefghijklmnopqrstuvwzyzabcdefghijklmnopqrstuvwzyzabcdefghijklmnopqrstuvwzyzabcdefghijklmnopqrstuvwzyzabcdefghijklmnopqrstuvwzyz"
	testEvent.EventName = testMigration.Name
	migrator = s.NewMigratorBackend(testMigration, testEvent)
	newEvent, err = migrator.ApplyMigration()
	if err == nil {
		t.Error("Test should have failed as Event Name is more than 127 chars")
		t.Logf("%+v", newEvent)
	}
}

func AddTableAddColumnOnly(t *testing.T) {
	t.Log("Testing to see if add table migration only has add column operations")

	testMigration := Migration1OnEvent1()
	testEvent := EventTest1Empty()
	migrator := s.NewMigratorBackend(testMigration, testEvent)
	newEvent, err := migrator.ApplyMigration()
	if err != nil {
		t.Errorf("Expected passed migration, instead errored: %s", err)
	} else {
		t.Logf("Passed migration successfully: %+v", newEvent)
	}

	testMigration.ColumnOperations[0].Operation = "update"
	migrator = s.NewMigratorBackend(testMigration, testEvent)
	newEvent, err = migrator.ApplyMigration()
	if err == nil {
		t.Error("Test should have failed as first column operation is an update")
		t.Logf("%+v", newEvent)
	}

	testMigration.ColumnOperations[0].Operation = "remove"
	migrator = s.NewMigratorBackend(testMigration, testEvent)
	newEvent, err = migrator.ApplyMigration()
	if err == nil {
		t.Error("Test should have failed as first column operation is a remove")
		t.Logf("%+v", newEvent)
	}
}

func AddTableAddColumnNoTransformerJunk(t *testing.T) {
	t.Log("Testing to see column transformer in migration is valid")

	testMigration := Migration1OnEvent1()
	testEvent := EventTest1Empty()
	migrator := s.NewMigratorBackend(testMigration, testEvent)
	newEvent, err := migrator.ApplyMigration()
	if err != nil {
		t.Errorf("Expected passed migration, instead errored: %s", err)
	} else {
		t.Logf("Passed migration successfully: %+v", newEvent)
	}

	testMigration.ColumnOperations[0].NewColumnDefinition.Transformer = "NotRealTransformer"
	migrator = s.NewMigratorBackend(testMigration, testEvent)
	newEvent, err = migrator.ApplyMigration()
	if err == nil {
		t.Error("Test should have failed as first column operation transformer is not valid")
		t.Logf("%+v", newEvent)
	}
}

func AddTableAddColumnVarCharLessThan64k(t *testing.T) {
	t.Log("Testing to see if column value varchar is less than 64k")

	testMigration := Migration1OnEvent1()
	testEvent := EventTest1Empty()
	migrator := s.NewMigratorBackend(testMigration, testEvent)
	newEvent, err := migrator.ApplyMigration()
	if err != nil {
		t.Errorf("Expected passed migration, instead errored: %s", err)
	} else {
		t.Logf("Passed migration successfully: %+v", newEvent)
	}

	testMigration.ColumnOperations[0].NewColumnDefinition.ColumnCreationOptions = "(65536)"
	migrator = s.NewMigratorBackend(testMigration, testEvent)
	newEvent, err = migrator.ApplyMigration()
	if err == nil {
		t.Error("Test should have failed as first column operation Column creation is higher than varchar byte max")
		t.Logf("%+v", newEvent)
	}

	testMigration.ColumnOperations[0].NewColumnDefinition.ColumnCreationOptions = "(65535)"
	migrator = s.NewMigratorBackend(testMigration, testEvent)
	newEvent, err = migrator.ApplyMigration()
	if err != nil {
		t.Errorf("Expected passed migration, instead errored: %s", err)
	} else {
		t.Logf("Passed migration successfully: %+v", newEvent)
	}
}

func AddTableAddColumnOutboundNameCollision(t *testing.T) {
	t.Log("Testing to see if new column creates name collision with existing column")

	testMigration := Migration1OnEvent1()
	testEvent := EventTest1Empty()
	migrator := s.NewMigratorBackend(testMigration, testEvent)
	newEvent, err := migrator.ApplyMigration()
	if err != nil {
		t.Errorf("Expected passed migration, instead errored: %s", err)
	} else {
		t.Logf("Passed migration successfully: %+v", newEvent)
	}

	testMigration.ColumnOperations[1].OutboundName = testMigration.ColumnOperations[0].OutboundName
	migrator = s.NewMigratorBackend(testMigration, testEvent)
	newEvent, err = migrator.ApplyMigration()
	if err == nil {
		t.Error("Test should have failed as second column operation outbound name collides with first")
		t.Logf("%+v", newEvent)
	}
}

func AddTableAddColumnColumnNameLen(t *testing.T) {
	t.Log("Testing to make sure column name len is less than redshift limit")
	testMigration := Migration1OnEvent1()
	testEvent := EventTest1Empty()
	migrator := s.NewMigratorBackend(testMigration, testEvent)
	newEvent, err := migrator.ApplyMigration()
	if err != nil {
		t.Errorf("Expected passed migration, instead errored: %s", err)
	} else {
		t.Logf("Passed migration successfully: %+v", newEvent)
	}

	testMigration.ColumnOperations[0].OutboundName = "abcdefghijklmnopqrstuvwzyzabcdefghijklmnopqrstuvwzyzabcdefghijklmnopqrstuvwzyzabcdefghijklmnopqrstuvwzyzabcdefghijklmnopqrstuvwzyz"
	migrator = s.NewMigratorBackend(testMigration, testEvent)
	newEvent, err = migrator.ApplyMigration()
	if err == nil {
		t.Error("Test should have failed as first column outbout name is longer than 127 chars")
		t.Logf("%+v", newEvent)
	}
}

func AddTableAddColumnColumnsMoreThan300(t *testing.T) {
	t.Log("Testing if there are more than 300 columns")

	testMigration := Migration1OnEvent1()
	testEvent := EventTest1Empty()
	migrator := s.NewMigratorBackend(testMigration, testEvent)
	newEvent, err := migrator.ApplyMigration()
	if err != nil {
		t.Errorf("Expected passed migration, instead errored: %s", err)
	} else {
		t.Logf("Passed migration successfully: %+v", newEvent)
	}

	for i := 5; i < 310; i++ {
		testMigration.ColumnOperations = append(testMigration.ColumnOperations,
			s.ColumnOperation{
				Operation:    "add",
				InboundName:  fmt.Sprintf("test_event_1_new_inbound_col_%d", i),
				OutboundName: fmt.Sprintf("test_event_1_new_inbound_col_%d", i),
				NewColumnDefinition: s.ColumnDefinition{
					Transformer:           "varchar",
					InboundName:           fmt.Sprintf("test_event_1_new_inbound_col_%d", i),
					OutboundName:          fmt.Sprintf("test_event_1_new_inbound_col_%d", i),
					ColumnCreationOptions: "(500)",
				},
			})
	}
	migrator = s.NewMigratorBackend(testMigration, testEvent)
	newEvent, err = migrator.ApplyMigration()
	if err == nil {
		t.Error("Test should have failed as there are more than 300 columns being added")
		t.Logf("%+v", newEvent)
	}
}
