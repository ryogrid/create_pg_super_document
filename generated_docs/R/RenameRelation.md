# RenameRelation

## Location
src/backend/commands/tablecmds.c: 4071 - 4134

## Overview
RenameRelation is the top-level function that handles renaming of database relations including tables, indexes, sequences, views, materialized views, and foreign tables.

## Definition
ObjectAddress RenameRelation(RenameStmt *stmt)

## Detailed Description
RenameRelation processes ALTER TABLE/INDEX/SEQUENCE/VIEW/MATERIALIZED VIEW/FOREIGN TABLE RENAME statements by first determining the appropriate lock mode based on the object type. It implements adaptive locking logic that initially assumes the statement type matches the actual object type, but detects mismatches and retries with the correct lock level if needed.

The function uses ShareUpdateExclusiveLock for index operations and AccessExclusiveLock for other relation types. It handles missing relations gracefully when the missing_ok flag is set, issuing a notice instead of an error. Once the correct lock is acquired, it delegates the actual renaming work to RenameRelationInternal.

## Parameters / Member Variables
- `stmt`: RenameStmt structure containing the rename operation details
  - `renameType`: Type of object being renamed (OBJECT_INDEX, OBJECT_TABLE, etc.)
  - `relation`: Target relation to rename
  - `newname`: New name for the relation
  - `missing_ok`: Whether to silently skip if relation does not exist

## Dependencies
- Functions called/Symbols referenced:
  - [RangeVarGetRelidExtended](RangeVarGetRelidExtended.md)
  - [RangeVarCallbackForAlterRelation](RangeVarCallbackForAlterRelation.md)
  - [get_rel_relkind](../g/get_rel_relkind.md)
  - [UnlockRelationOid](../U/UnlockRelationOid.md)
  - [RenameRelationInternal](RenameRelationInternal.md)
  - ObjectAddressSet
  - ereport/NOTICE
- Called from (representative examples):
  - [ExecRenameStmt](../E/ExecRenameStmt.md) (in src/backend/commands/alter.c)

## Notes and Other Information
- Uses adaptive locking strategy to handle statement/object type mismatches (e.g., ALTER INDEX on a table)
- Retains exclusive locks until end of transaction to prevent concurrent modifications
- Lock levels match those used by RenameRelationInternal to avoid lock escalation
- Supports renaming of various relation types through a single interface
- Uses different callback function (RangeVarCallbackForAlterRelation) than attribute renaming
- Returns InvalidObjectAddress if the relation does not exist and missing_ok is true
- Part of the broader relation management infrastructure in PostgreSQL