# ATExecAlterConstraint

## Location
[src/backend/commands/tablecmds.c:11416-11552](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L11416-L11552)

## Overview
ATExecAlterConstraint updates the attributes of a constraint in PostgreSQL, specifically implementing the ALTER TABLE ALTER CONSTRAINT command. Currently it only works for Foreign Key constraints.

## Definition

```c
static ObjectAddress
ATExecAlterConstraint(Relation rel, AlterTableCmd *cmd, bool recurse,
					  bool recursing, LOCKMODE lockmode)
```
## Detailed Description
This function modifies constraint attributes such as deferrability and initial deferred status for foreign key constraints. It performs several validation checks including ensuring the constraint exists, is a foreign key constraint, and is a top-level constraint (not inherited). The function handles both regular tables and partitioned tables, with special logic for partitioned tables where partitions need processing regardless of whether the constraint attributes actually changed.

The function follows these main steps:
1. Opens the constraint and trigger system catalogs
2. Searches for the target constraint by name and relation
3. Validates the constraint type (must be foreign key)
4. Ensures it's a top-level constraint (not inherited from a parent)
5. Calls ATExecAlterConstrRecurse to perform the actual modification
6. Invalidates relation caches for affected relations

## Parameters / Member Variables
- : The relation containing the constraint to be altered
- : The ALTER TABLE command containing constraint modification details
- : Whether to recursively apply changes to child tables
- : Whether this call is part of a recursive operation
- : The lock mode to use for the operation

## Dependencies
- Functions called/Symbols referenced:
  - castNode
  - table_open
  - [ScanKeyInit](../S/ScanKeyInit.md)
  - [systable_beginscan](../s/systable_beginscan.md)
  - [systable_getnext](../s/systable_getnext.md)
  - HeapTupleIsValid
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - [ATExecAlterConstrRecurse](ATExecAlterConstrRecurse.md)
  - ObjectAddressSet
  - [CacheInvalidateRelcacheByRelid](../C/CacheInvalidateRelcacheByRelid.md)
  - [get_rel_name](../g/get_rel_name.md)
- Called from (representative examples):
  - [ATExecCmd](ATExecCmd.md) (main ALTER TABLE command dispatcher)

## Notes and Other Information
- Only supports foreign key constraints; other constraint types will result in an error
- Inherited constraints cannot be altered directly - the user must alter the parent constraint instead
- For partitioned tables, all partitions are processed even if the constraint attributes don't change
- The function maintains referential integrity by invalidating caches for all affected relations
- Returns InvalidObjectAddress if no changes were made, otherwise returns the constraint's ObjectAddress