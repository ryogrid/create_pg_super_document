# ATExecAddIndexConstraint

## Location
src/backend/commands/tablecmds.c: 9263 - 9354

## Overview
ATExecAddIndexConstraint implements the ALTER TABLE ADD CONSTRAINT USING INDEX command, which creates a primary key or unique constraint using an existing unique index.

## Definition


## Detailed Description
This function executes the ALTER TABLE ADD CONSTRAINT USING INDEX operation by taking an existing unique index and converting it into a table constraint (either PRIMARY KEY or UNIQUE). The function validates that the specified index is unique, handles constraint naming (renaming the index if necessary to match the constraint name), and creates the appropriate catalog entries. It ensures the constraint and index have the same name as required by PostgreSQL's design.

The function performs several key validations: it rejects operations on partitioned tables (not currently supported), verifies the index is unique, and performs additional checks for primary key constraints. When creating primary key constraints, it calls index_check_primary_key to ensure all necessary conditions are met.

## Parameters / Member Variables
- : AlteredTableInfo structure containing information about the table being altered
- : Relation object representing the table to which the constraint is being added
- : IndexStmt containing the constraint specification, including the index OID and constraint properties
- : Lock mode to use during the operation (though not directly used in this function)

## Dependencies
- Functions called/Symbols referenced:
  - [index_open](../i/index_open.md)
  - [BuildIndexInfo](../B/BuildIndexInfo.md)
  - [RenameRelationInternal](../R/RenameRelationInternal.md)
  - [index_check_primary_key](../i/index_check_primary_key.md)
  - [index_constraint_create](../i/index_constraint_create.md)
  - [index_close](../i/index_close.md)
- Called from (representative examples):
  - [ATExecCmd](ATExecCmd.md) (main ALTER TABLE command dispatcher)

## Notes and Other Information
- Currently does not support partitioned tables and will raise an error if attempted
- Requires the underlying index to be unique, which should be validated at parse time
- Enforces naming consistency between constraints and indexes by renaming the index if necessary
- Supports both PRIMARY KEY and UNIQUE constraints, but not EXCLUSION constraints
- Handles deferred and deferrable constraint options through appropriate flags
- Returns an ObjectAddress for the newly created constraint for dependency tracking