package schema

import (
	"fmt"
	"strconv"
	"strings"
)

const add string = "add"
const update string = "update"
const remove string = "remove"
const maxVarcharLen int = 65535

//Migrator interface that abstracts the migration method
type Migrator interface {
	ApplyMigration() (*Event, error)
}

//Event stores all the relevant data in event
type Event struct {
	EventName       string
	Version         int
	Columns         []ColumnDefinition
	ParentMigration Migration
	TableOption     TableOption
}

//ColumnDefinition stores all the relevant data for a single column definition
type ColumnDefinition struct {
	InboundName           string
	OutboundName          string
	Transformer           string
	ColumnCreationOptions string
}

//Migration stores all the relevant data for a migration
type Migration struct {
	TableOperation   string
	Name             string
	ColumnOperations []ColumnOperation
	TableOption      TableOption
}

//ColumnOperation stores all the relevant data for a single column operation
type ColumnOperation struct {
	Operation           string
	InboundName         string
	OutboundName        string
	NewColumnDefinition ColumnDefinition
}

//TableOption stores all the relevant data for the options a table may store
type TableOption struct {
	DistKey []string
	SortKey []string
}

//NewEvent returns an empty event with specified eventName and eventVersion
func NewEvent(eventName string, eventVersion int) Event {
	return Event{
		EventName: eventName,
		Version:   eventVersion,
	}
}

//IsEmpty returns (t/f) whether or not if the Event is empty or not
func (e *Event) IsEmpty() bool {
	return len(e.Columns) == 0
}

//IsEmpty returns (t/f) whether or not if the Tableoption is empty or not
func (to *TableOption) IsEmpty() bool {
	return len(to.DistKey) == 0 && len(to.SortKey) == 0
}

func (m *Migration) IsRemoveEvent() bool {
	return m.TableOperation == remove
}

func (e *Event) addColumn(ColumnOperation ColumnOperation) error {
	if ColumnOperation.Operation != add {
		return ErrColumnOpNotAdd
	}

	//contains valid transformer
	if !TransformList.Contains(ColumnOperation.NewColumnDefinition.Transformer) {
		return ErrInvalidTransformer
	}

	//checks if varchar byte limit is exceeded
	if ColumnOperation.NewColumnDefinition.Transformer == "varchar" {
		temp := ColumnOperation.NewColumnDefinition.ColumnCreationOptions
		varcharLen, err := strconv.Atoi(temp[1 : len(temp)-1])
		if err != nil {
			return ErrVarCharNotInt
		}
		if varcharLen > maxVarcharLen {
			return ErrVarCharBytesMax
		}
	}

	//checks if outbound column name is valid identifier
	if !IsValidIdentifier(ColumnOperation.OutboundName) {
		return fmt.Errorf("Invalid identifier for Column Name: %s", ColumnOperation.OutboundName)
	}

	// Check for column name collision, and add column, return error if there is one
	for _, column := range e.Columns {
		if column.OutboundName == ColumnOperation.OutboundName {
			return ErrOutboundNameCollision
		}
	}
	e.Columns = append(e.Columns, ColumnOperation.NewColumnDefinition)
	return nil
}

func (e *Event) removeColumn(ColumnOperation ColumnOperation) error {
	//column operation is remove
	if ColumnOperation.Operation != remove {
		return ErrColumnOpNotRemove
	}

	//finds index in list which corresponds to column that needs to be removed
	i := -1
	for index, column := range e.Columns {
		if column.OutboundName == ColumnOperation.OutboundName {
			i = index
			break
		}
	}

	//checks to see if column event existed to begin with
	if i == -1 {
		return ErrRemoveColNonExistingCol
	}

	//cannot remove columns that are distkey
	for _, key := range e.TableOption.DistKey {
		if key == ColumnOperation.OutboundName {
			return ErrRemoveColisDistKey
		}
	}

	e.Columns = append(e.Columns[:i], e.Columns[i+1:]...)
	return nil
}

func (e *Event) updateColumn(ColumnOperation ColumnOperation) error {
	//column operation is update
	if ColumnOperation.Operation != update {
		return ErrColumnOpNotUpdate
	}

	//Check if transformer is valid
	if !TransformList.Contains(ColumnOperation.NewColumnDefinition.Transformer) {
		return ErrInvalidTransformer
	}

	//checks if varchar byte limit is exceeded
	if ColumnOperation.NewColumnDefinition.Transformer == "varchar" {
		temp := ColumnOperation.NewColumnDefinition.ColumnCreationOptions
		varcharLen, err := strconv.Atoi(temp[1 : len(temp)-1])
		if err != nil {
			return ErrVarCharNotInt
		}
		if varcharLen > maxVarcharLen {
			return ErrVarCharBytesMax
		}
	}

	//checks if outbound col name is valid identifier
	if !IsValidIdentifier(ColumnOperation.NewColumnDefinition.OutboundName) {
		return fmt.Errorf("Invalid identifier for Column Name: %s", ColumnOperation.NewColumnDefinition.OutboundName)
	}

	//cannot update columns that are distkey
	for _, key := range e.TableOption.DistKey {
		if key == ColumnOperation.OutboundName {
			return ErrUpdateColisDistKey
		}
	}

	//finds index in list which corresponds to column that needs to be updated
	i := -1

	for index, column := range e.Columns {
		if column.OutboundName == ColumnOperation.OutboundName {
			i = index
		} else {
			if column.OutboundName == ColumnOperation.NewColumnDefinition.OutboundName {
				return ErrOutboundNameCollision
			}
		}
	}

	//checks to see if column event existed to begin with
	if i == -1 {
		return ErrUpdateColNonExistingCol
	}

	e.Columns[i] = ColumnOperation.NewColumnDefinition
	return nil
}

//HashSet is a map struct designed to be used as a set
type HashSet map[string]HashMember

//HashMember is an empty struct used as values in HashSet
type HashMember struct{}

//Contains determins if val is in the corresponding HashSet
func (hs HashSet) Contains(val string) bool {
	_, ok := hs[val]
	return ok
}

//Delete deletes the corresponding item from the hashset if it exists.
func (hs HashSet) Delete(val string) {
	delete(hs, val)
}

//TransformList contains all the correct transformer items in a HashSet
var TransformList = HashSet{
	"bigint":             HashMember{},
	"float":              HashMember{},
	"varchar":            HashMember{},
	"ipAsnInteger":       HashMember{},
	"int":                HashMember{},
	"bool":               HashMember{},
	"ipCity":             HashMember{},
	"ipCountry":          HashMember{},
	"ipRegion":           HashMember{},
	"ipAsn":              HashMember{},
	"stringToIntegerMD5": HashMember{},
	"f@timestamp@unix":   HashMember{},
}

//IsValidIdentifier determines if a given identifier is valid in redshift
func IsValidIdentifier(identifier string) bool {
	if len(identifier) > 127 || len(identifier) < 1 {
		return false
	}

	if strings.ContainsAny(identifier, "\x00") {
		return false
	}

	return true
}

//NewOutboundColsHashSet creates a hashset of the outbound columns in a migration for quickly checking inclusion of distkeys and sortkeys
func (m *Migration) NewOutboundColsHashSet() HashSet {
	outboundColNames := make(HashSet)
	for _, operation := range m.ColumnOperations {
		outboundColNames[operation.OutboundName] = HashMember{}
	}
	return outboundColNames
}
