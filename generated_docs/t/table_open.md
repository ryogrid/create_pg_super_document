# table_open

## Location
[src/backend/access/table/table.c:40-59](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/table/table.c#L40-L59)

## Overview
Opens a table relation by its object identifier (OID) and ensures that the relation is specifically a table, not an index or composite type.

## Definition
```c
Relation table_open(Oid relationId, LOCKMODE lockmode);
```

## Detailed Description
`table_open` is essentially a wrapper around `relation_open` that adds validation to ensure the opened relation is a table. It performs the following actions:
1. Opens the relation using the provided OID and lock mode via `relation_open`
2. Validates that the relation is not an index or composite type using `validate_relation_kind`
3. Returns the opened relation

The function provides a convenient and safe way to open table relations while ensuring type safety. The caller should still verify that the relation is not a view or foreign table before assuming it has physical storage.

## Parameters / Member Variables
- `relationId`: Object identifier (OID) of the relation to open
- `lockmode`: Type of lock to acquire on the relation (e.g., AccessShareLock, RowExclusiveLock)

## Dependencies
- Functions called/Symbols referenced:
  - relation_open
  - validate_relation_kind
- Called from (representative examples):
  - Various BRIN index operations
  - Table manipulation functions across the codebase

## Notes and Other Information
- This function adds type safety over the generic `relation_open` by ensuring only tables are opened
- Callers should still check if the relation is a view or foreign table before assuming physical storage
- The function will raise an error if the relation is an index or composite type
- Part of the table access method interface introduced to abstract table operations