# RenameConstraint

## Location
src/backend/commands/tablecmds.c: 4021 - 4070

## Overview
RenameConstraint is the top-level function that handles renaming constraints on both relations and domains, serving as the main entry point for ALTER TABLE/DOMAIN RENAME CONSTRAINT commands.

## Definition
ObjectAddress RenameConstraint(RenameStmt *stmt)

## Detailed Description
RenameConstraint processes RENAME CONSTRAINT statements by first determining whether the target is a domain constraint or table constraint, then performing the appropriate validation and locking operations. For domain constraints, it validates domain ownership permissions. For table constraints, it acquires an exclusive lock on the relation and handles missing relations gracefully when the missing_ok flag is set.

The function delegates the actual renaming work to rename_constraint_internal, passing along the appropriate parameters including recursion settings based on the inheritance flag. It returns an ObjectAddress identifying the renamed constraint for dependency tracking and event triggers.

## Parameters / Member Variables
- `stmt`: RenameStmt structure containing the rename operation details
  - `renameType`: Type of object being renamed (OBJECT_DOMCONSTRAINT for domains)
  - `object`: Target domain name (for domain constraints) or NULL
  - `relation`: Target relation (for table constraints) or NULL
  - `subname`: Current name of the constraint to rename
  - `newname`: New name for the constraint
  - `missing_ok`: Whether to silently skip if relation does not exist
  - `behavior`: Drop behavior (not used in constraint renaming)

## Dependencies
- Functions called/Symbols referenced:
  - [typenameTypeId](../t/typenameTypeId.md)
  - makeTypeNameFromNameList
  - table_open/table_close
  - [checkDomainOwner](../c/checkDomainOwner.md)
  - [RangeVarGetRelidExtended](RangeVarGetRelidExtended.md)
  - [RangeVarCallbackForRenameAttribute](RangeVarCallbackForRenameAttribute.md)
  - [rename_constraint_internal](../r/rename_constraint_internal.md)
  - ereport/NOTICE
- Called from (representative examples):
  - [ExecRenameStmt](../E/ExecRenameStmt.md) (in src/backend/commands/alter.c)

## Notes and Other Information
- Handles both domain constraints and table/relation constraints in a single function
- Uses AccessExclusiveLock for relation constraints to prevent concurrent modifications
- Domain constraint renaming requires domain ownership validation
- Uses RowExclusiveLock when accessing the type catalog for domain constraints
- Returns InvalidObjectAddress if the relation does not exist and missing_ok is true
- Passes inheritance information from the RangeVar to control recursive constraint renaming
- Part of the broader constraint management infrastructure in PostgreSQL