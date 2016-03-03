package schema

import "errors"

//errors that can be recognized for testing
var (
	ErrInvalidTableOperation         = errors.New("Not one of the valid table operations (add, remove, update)")
	ErrInvalidColumnOperation        = errors.New("Not one of the valid column operations (add, remove, update)")
	ErrAddTableOnExistingTable       = errors.New("Cannot Add table that already exists")
	ErrRemoveTableOnNonExistingTable = errors.New("Cannot Remove Table that doesn't exist")
	ErrUpdateTableonNonExistingTable = errors.New("Cannot Update Table that doesn't exist")
	ErrMustContainDistKey            = errors.New("Cannot proceed without having atleast a single DistKey")
	ErrDistKeyNotInCols              = errors.New("DistKey must be present in outbound col names")
	ErrSortKeyNotInCols              = errors.New("SortKey must be present in outbound col names")
	ErrDifferentTableOptions         = errors.New("Cannot change Table Options on update")
	ErrRemoveColisDistKey            = errors.New("Remove Column operation is on DistKey")
	ErrUpdateColisDistKey            = errors.New("Update Column operation is on DistKey")
	ErrUpdateColNonExistingCol       = errors.New("Cannot Update column that does not exist")
	ErrRemoveColNonExistingCol       = errors.New("Cannot Remove column that does not exist")
	ErrColumnOpNotAdd                = errors.New("Columnn Operation should be Add")
	ErrColumnOpNotUpdate             = errors.New("Columnn Operation should be Update")
	ErrColumnOpNotRemove             = errors.New("Columnn Operation should be Remove")
	ErrInvalidTransformer            = errors.New("Invalid Transformer")
	ErrVarCharBytesMax               = errors.New("varchar size exceeds max (65546), 64k-1")
	ErrVarCharNotInt                 = errors.New("varchar option provided not an int")
	ErrOutboundNameCollision         = errors.New("Outbound Column name collides with existing column")
	ErrTooManyColumns                = errors.New("Having more than 300 columns in a redshift table slows queries immensely")
	ErrMigrationNameDoesNotMatch     = errors.New("Migration name does not match the name of the event it is migrating")
)
