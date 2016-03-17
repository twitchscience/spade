package schema

import (
	"errors"
	"fmt"
	"reflect"
)

//MigratorBackend stores the migration and event objects for a migration
type MigratorBackend struct {
	possibleMigration *Migration
	currentEvent      *Event
}

//NewMigratorBackend is the constructor for applying a new migration
func NewMigratorBackend(newMigration Migration, currentEvent Event) MigratorBackend {
	return MigratorBackend{
		possibleMigration: &newMigration,
		currentEvent:      &currentEvent,
	}
}

//ApplyMigration calls the necessary functions to apply a migration to an event, or error
func (m *MigratorBackend) ApplyMigration() (Event, error) {
	if m.possibleMigration.Name != m.currentEvent.EventName {
		return Event{}, ErrMigrationNameDoesNotMatch
	}

	switch m.possibleMigration.TableOperation {
	case add:
		return m.addTable()
	case remove:
		return m.removeTable()
	case update:
		return m.updateTable()
	default:
		return Event{}, ErrInvalidTableOperation
	}
}

func (m *MigratorBackend) addTable() (Event, error) {
	//checks to see if table already exists.
	if !m.currentEvent.IsEmpty() {
		return Event{}, ErrAddTableOnExistingTable
	}
	//checks for existance of atleast single distKey
	if len(m.possibleMigration.TableOption.DistKey) < 1 {
		return Event{}, ErrMustContainDistKey
	}

	//checks if distkey and sortkey are actually in the columns
	outboundCols := m.possibleMigration.NewOutboundColsHashSet()
	for _, distKey := range m.possibleMigration.TableOption.DistKey {
		if !outboundCols.Contains(distKey) {
			return Event{}, ErrDistKeyNotInCols
		}
	}
	for _, sortKey := range m.possibleMigration.TableOption.SortKey {
		if !outboundCols.Contains(sortKey) {
			return Event{}, ErrSortKeyNotInCols
		}
	}

	if !IsValidIdentifier(m.possibleMigration.Name) {
		return Event{}, fmt.Errorf("Invalid identifier for Migration Name: %s", m.possibleMigration.Name)
	}

	if len(m.possibleMigration.ColumnOperations) > 300 {
		return Event{}, ErrTooManyColumns
	}

	//in the process of adding columns, validate add columns as well.
	for _, ColumnOperation := range m.possibleMigration.ColumnOperations {
		err := m.currentEvent.addColumn(ColumnOperation)

		if err != nil {
			return Event{}, errors.New("Adding Table failed: " + err.Error())
		}
	}

	//add table options to new event
	m.currentEvent.TableOption = m.possibleMigration.TableOption

	//increment version number
	m.currentEvent.Version++
	m.currentEvent.ParentMigration = *m.possibleMigration
	return *m.currentEvent, nil
}

func (m *MigratorBackend) removeTable() (Event, error) {
	//checks to see if table is already empty.
	if m.currentEvent.IsEmpty() {
		return Event{}, ErrRemoveTableOnNonExistingTable
	}

	m.currentEvent.Columns = []ColumnDefinition{}
	m.currentEvent.TableOption = TableOption{}

	m.currentEvent.Version++
	m.currentEvent.ParentMigration = *m.possibleMigration
	return *m.currentEvent, nil
}

func (m *MigratorBackend) updateTable() (Event, error) {
	//checks to see if table is already empty.
	if m.currentEvent.IsEmpty() {
		return Event{}, ErrUpdateTableonNonExistingTable
	}

	if !reflect.DeepEqual(m.possibleMigration.TableOption, m.currentEvent.TableOption) {
		return Event{}, ErrDifferentTableOptions
	}

	for _, ColumnOperation := range m.possibleMigration.ColumnOperations {

		var err error

		switch ColumnOperation.Operation {
		case add:
			err = m.currentEvent.addColumn(ColumnOperation)
		case remove:
			err = m.currentEvent.removeColumn(ColumnOperation)
		case update:
			err = m.currentEvent.updateColumn(ColumnOperation)
		default:
			err = ErrInvalidColumnOperation //in case column operation string is mangled
		}

		if err != nil {
			return Event{}, errors.New("Updating Table failed: " + err.Error())
		}
	}

	if len(m.currentEvent.Columns) > 300 {
		return Event{}, ErrTooManyColumns
	}

	if len(m.possibleMigration.TableOption.DistKey) < 1 {
		return Event{}, ErrMustContainDistKey
	}

	//table Option check? before or after migration? Still too consider.

	m.currentEvent.Version++
	m.currentEvent.ParentMigration = *m.possibleMigration
	return *m.currentEvent, nil
}
