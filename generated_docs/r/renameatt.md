# renameatt

## Location
src/backend/commands/tablecmds.c: 3877 - 3914

## Overview
renameatt is the top-level function that handles the renaming of an attribute (column) in a PostgreSQL relation, serving as the main entry point for ALTER TABLE RENAME COLUMN commands.

## Definition
ObjectAddress renameatt(RenameStmt *stmt)

## Detailed Description
renameatt processes a RENAME COLUMN statement by first acquiring an exclusive lock on the target relation, then delegating the actual renaming work to renameatt_internal. The function handles missing relations gracefully when the missing_ok flag is set, issuing a notice instead of an error. It performs the necessary permission checks through the RangeVarCallbackForRenameAttribute callback before proceeding with the rename operation.

The function returns an ObjectAddress that identifies the renamed column, making it suitable for dependency tracking and event triggers. The lock level used (AccessExclusiveLock) matches that used by renameatt_internal to ensure consistency across the operation.

## Parameters / Member Variables
- stmt: RenameStmt structure containing the rename operation details
  - relation: The target relation to modify
  - subname: Current name of the attribute to rename
  - newname: New name for the attribute
  - missing_ok: Whether to silently skip if relation does not exist
  - behavior: Drop behavior (CASCADE or RESTRICT)

## Dependencies
- Functions called/Symbols referenced:
  - RangeVarGetRelidExtended
  - AccessExclusiveLock
  - RVR_MISSING_OK
  - RangeVarCallbackForRenameAttribute
  - renameatt_internal
  - ObjectAddressSubSet
  - ereport/NOTICE
- Called from (representative examples):
  - ExecRenameStmt (in src/backend/commands/alter.c)

## Notes and Other Information
- Uses AccessExclusiveLock to prevent concurrent modifications during the rename operation
- Handles inheritance hierarchies through the inh flag passed to renameatt_internal
- Returns InvalidObjectAddress if the relation does not exist and missing_ok is true
- The function is designed to be called from the SQL command execution path
- Part of the broader table command infrastructure in PostgreSQL