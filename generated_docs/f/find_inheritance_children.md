# find_inheritance_children

## Location
src/backend/catalog/pg_inherits.c: 58 - 81

## Overview
Returns a list of OIDs for all relations that directly inherit from a specified parent relation, with automatic locking and exclusion of detached partitions.

## Definition


## Detailed Description
This function is a convenience wrapper around  that provides the most common inheritance child lookup behavior. It finds all relations that inherit *directly* from the specified parent relation, automatically excluding partitions marked as being detached. The function acquires the specified lock type on each child relation (but not on the parent relation, which should already be locked by the caller).

This is the standard function used throughout PostgreSQL for inheritance hierarchy traversal when detached partitions should be omitted and no additional filtering is needed.

## Parameters / Member Variables
- : OID of the parent relation whose direct children should be found
- : Lock mode to acquire on each child relation; use NoLock if no locking is desired (but beware of race conditions with concurrent DROP operations)

## Dependencies
- Functions called/Symbols referenced:
  - find_inheritance_children_extended
- Called from (representative examples):
  - find_all_inheritors (src/backend/catalog/pg_inherits.c:292)
  - renameatt_internal (src/backend/commands/tablecmds.c:3781)
  - rename_constraint_internal (src/backend/commands/tablecmds.c:3980)
  - ATExecAddColumn (src/backend/commands/tablecmds.c:7146, 7386)
  - ATExecDropColumn (src/backend/commands/tablecmds.c:9067)
  - ATAddCheckConstraint (src/backend/commands/tablecmds.c:9556)
  - ATExecDropConstraint (src/backend/commands/tablecmds.c:12680)
  - ATPrepAlterColumnType (src/backend/commands/tablecmds.c:13074)

## Notes and Other Information
- This function excludes partitions marked as being detached, making it suitable for most inheritance operations
- The caller is responsible for locking the parent relation before calling this function
- When lockmode is NoLock, callers must handle potential race conditions with concurrent DROP operations on child relations
- For more control over partition inclusion/exclusion and additional filtering, use find_inheritance_children_extended directly
- Located in src/backend/catalog/pg_inherits.c:58-81