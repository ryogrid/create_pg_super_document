# table_openrv

## Location
[src/backend/access/table/table.c:83-102](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/table/table.c#L83-L102)

## Overview
Opens a table relation specified by a RangeVar (relation name with optional schema qualification) and ensures that the relation is specifically a table, not an index or composite type.

## Definition
```c
Relation table_openrv(const RangeVar *relation, LOCKMODE lockmode);
```

## Detailed Description
`table_openrv` is a wrapper around `relation_openrv` that adds table-specific validation. It performs the following actions:
1. Opens the relation using the provided RangeVar and lock mode via `relation_openrv`
2. Validates that the relation is not an index or composite type using `validate_relation_kind`
3. Returns the opened relation

This function provides a convenient way to open table relations by name (with optional schema qualification) while ensuring type safety. The RangeVar structure allows for flexible relation specification including schema qualification and alias handling.

## Parameters / Member Variables
- `relation`: Pointer to a RangeVar structure containing the relation name and optional schema qualification
- `lockmode`: Type of lock to acquire on the relation (e.g., AccessShareLock, RowExclusiveLock)

## Dependencies
- Functions called/Symbols referenced:
  - [relation_openrv](../r/relation_openrv.md)
  - [validate_relation_kind](../v/validate_relation_kind.md)
- Types referenced:
  - [RangeVar](../R/RangeVar.md)
- Called from (representative examples):
  - SQL command processing functions that work with table names
  - DDL operations that need to open tables by name

## Notes and Other Information
- Provides name-based table opening with the same type safety as `table_open`
- Uses RangeVar for flexible relation naming including schema qualification
- Will raise an error if the relation does not exist or is not a table
- Part of the table access method interface for name-based table operations
- Callers should still verify the relation is not a view or foreign table before assuming physical storage