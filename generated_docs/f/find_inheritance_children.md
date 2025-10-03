# find_inheritance_children

## Location
[src/backend/catalog/pg_inherits.c:58-81](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_inherits.c#L58-L81)

## Overview
Returns a list of OIDs for all relations that directly inherit from a specified parent relation, with automatic locking and exclusion of detached partitions.

## Definition

```c
List *
find_inheritance_children(Oid parentrelId, LOCKMODE lockmode)
```
## Detailed Description
This function is a convenience wrapper around  that provides the most common inheritance child lookup behavior. It finds all relations that inherit *directly* from the specified parent relation, automatically excluding partitions marked as being detached. The function acquires the specified lock type on each child relation (but not on the parent relation, which should already be locked by the caller).

This is the standard function used throughout PostgreSQL for inheritance hierarchy traversal when detached partitions should be omitted and no additional filtering is needed.

## Parameters / Member Variables
- `parentrelId`: OID of the parent relation whose direct children should be found
- `lockmode`: Lock mode to acquire on each child relation; use NoLock if no locking is desired (but beware of race conditions with concurrent DROP operations)
## Dependencies
- Functions called/Symbols referenced:
  - [find_inheritance_children_extended](find_inheritance_children_extended.md)
- Called from (representative examples):
  - [find_all_inheritors](find_all_inheritors.md) (src/backend/catalog/pg_inherits.c:292)
  - [renameatt_internal](../r/renameatt_internal.md) (src/backend/commands/tablecmds.c:3781)
  - [rename_constraint_internal](../r/rename_constraint_internal.md) (src/backend/commands/tablecmds.c:3980)
  - [ATExecAddColumn](../A/ATExecAddColumn.md) (src/backend/commands/tablecmds.c:7146, 7386)
  - [ATExecDropColumn](../A/ATExecDropColumn.md) (src/backend/commands/tablecmds.c:9067)
  - [ATAddCheckConstraint](../A/ATAddCheckConstraint.md) (src/backend/commands/tablecmds.c:9556)
  - [ATExecDropConstraint](../A/ATExecDropConstraint.md) (src/backend/commands/tablecmds.c:12680)
  - [ATPrepAlterColumnType](../A/ATPrepAlterColumnType.md) (src/backend/commands/tablecmds.c:13074)

## Notes and Other Information
- This function excludes partitions marked as being detached, making it suitable for most inheritance operations
- The caller is responsible for locking the parent relation before calling this function
- When lockmode is NoLock, callers must handle potential race conditions with concurrent DROP operations on child relations
- For more control over partition inclusion/exclusion and additional filtering, use find_inheritance_children_extended directly
- Located in src/backend/catalog/pg_inherits.c:58-81

## Simplified Source

```c
List *find_inheritance_children(Oid parentrelId, LOCKMODE lockmode) {
    // Simple wrapper that finds direct inheritance children
    // Excludes detached partitions by default
    return find_inheritance_children_extended(parentrelId, true, lockmode, NULL, NULL);
}
```