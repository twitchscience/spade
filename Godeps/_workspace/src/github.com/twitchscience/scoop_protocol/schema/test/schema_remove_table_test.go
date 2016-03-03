package test

import (
	"testing"

	s "github.com/twitchscience/scoop_protocol/schema"
)

func TestRemoveTable(t *testing.T) {
	RemoveTableIsColumnSchemaFull(t)
}

func RemoveTableIsColumnSchemaFull(t *testing.T) {
	t.Log("Testing if the table is actually occupied")

	testMigration := Migration2OnEvent1()
	testEvent := EventTest1()
	migrator := s.NewMigratorBackend(testMigration, testEvent)
	newEvent, err := migrator.ApplyMigration()
	if err != nil {
		t.Errorf("Expected passed migration, instead errored: %s", err)
	} else {
		t.Logf("Passed migration successfully: %+v", newEvent)
	}

	testEvent = EventTest1Empty()
	migrator = s.NewMigratorBackend(testMigration, testEvent)
	newEvent, err = migrator.ApplyMigration()
	if err == nil {
		t.Error("Test should have failed as event is already empty")
		t.Logf("%+v", newEvent)
	}
}
