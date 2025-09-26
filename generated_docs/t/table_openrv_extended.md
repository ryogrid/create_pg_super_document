# table_openrv_extended

## Location
[src/backend/access/table/table.c:103-125](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/table/table.c#L103-L125)

## Overview
Extended version of table_openrv that allows graceful handling of missing relations by optionally returning NULL instead of raising an error when the relation does not exist.

## Definition
```c
Relation table_openrv_extended(const RangeVar *relation, LOCKMODE lockmode, bool missing_ok);
```

## Detailed Description
`table_openrv_extended` is an extended wrapper around `relation_openrv_extended` that provides both name-based table opening and flexible error handling. It performs the following actions:
1. Opens the relation using the provided RangeVar, lock mode, and missing_ok flag via `relation_openrv_extended`
2. If the relation was successfully opened, validates that it is not an index or composite type using `validate_relation_kind`
3. Returns the opened relation or NULL based on the missing_ok parameter and relation existence

This function combines the flexibility of name-based relation specification with optional graceful handling of missing relations, making it suitable for scenarios where table existence is uncertain.

## Parameters / Member Variables
- `relation`: Pointer to a RangeVar structure containing the relation name and optional schema qualification
- `lockmode`: Type of lock to acquire on the relation (e.g., AccessShareLock, RowExclusiveLock)
- `missing_ok`: Boolean flag indicating whether to return NULL (true) or raise an error (false) when the relation does not exist

## Dependencies
- Functions called/Symbols referenced:
  - relation_openrv_extended
  - validate_relation_kind
- Types referenced:
  - RangeVar
- Called from (representative examples):
  - SQL commands that need to handle potentially missing tables gracefully
  - Administrative operations that should continue if a table does not exist

## Notes and Other Information
- Combines name-based opening with graceful error handling based on the missing_ok parameter
- Still validates relation type if the relation exists, ensuring type safety
- Returns NULL when missing_ok=true and the relation does not exist
- Raises an error when missing_ok=false and the relation does not exist
- Part of the table access method interface providing maximum flexibility for table opening
- Most flexible table opening function, suitable for conditional operations