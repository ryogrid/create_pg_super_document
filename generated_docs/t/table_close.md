# table_close

## Location
[src/backend/access/table/table.c:126-137](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/table/table.c#L126-L137)

## Overview
Closes a table relation and optionally releases the specified lock, serving as the table-specific counterpart to the table opening functions.

## Definition
```c
void table_close(Relation relation, LOCKMODE lockmode);
```

## Detailed Description
`table_close` is a simple wrapper around `relation_close` that provides a table-specific interface for closing relations. It performs the following actions:
1. Closes the relation using `relation_close` with the provided lock mode
2. If lockmode is not NoLock, releases the specified lock on the relation

The function provides symmetry with the table_open family of functions and maintains the table access method interface abstraction. It is often sensible to hold locks beyond the relation close operation, in which case the lock will be automatically released at transaction end.

## Parameters / Member Variables
- `relation`: Pointer to the Relation structure representing the table to close
- `lockmode`: Type of lock to release (e.g., AccessShareLock, RowExclusiveLock, or NoLock to keep the lock)

## Dependencies
- Functions called/Symbols referenced:
  - [relation_close](../r/relation_close.md)
- Called from (representative examples):
  - All functions that previously called table_open variants
  - Table manipulation and query processing functions
  - Administrative and maintenance operations

## Notes and Other Information
- Simple wrapper around relation_close providing table-specific interface consistency
- Part of the table access method interface for clean abstraction
- Locks are automatically released at transaction end if not explicitly released
- Should be called to properly close tables opened with any table_open variant
- Does not perform additional table-specific validation like the opening functions
- Essential for proper resource management and lock cleanup

## Simplified Source

```c
void table_close(Relation relation, LOCKMODE lockmode) {
    // Close the table and release specified lock
    relation_close(relation, lockmode);
}
```