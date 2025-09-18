# pg_table_size

## Location
src/backend/utils/adt/dbsize.c: 486 - 504

## Overview
SQL-callable function that returns the disk space used by the specified table, excluding indexes but including TOAST data, FSM, and VM.

## Definition
```c
Datum pg_table_size(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as the SQL interface for PostgreSQL's `pg_table_size()` system function. It provides a safe, user-accessible way to query table storage size by:

1. **Input validation**: Extracts the relation OID from function arguments
2. **Relation access**: Opens the relation using `try_relation_open` with AccessShareLock for safe concurrent access
3. **Error handling**: Returns NULL if the relation cannot be opened (e.g., doesn't exist, no permissions)
4. **Size calculation**: Delegates to `calculate_table_size` for the actual computation
5. **Resource cleanup**: Properly closes the relation and releases the lock
6. **Result formatting**: Returns the size as a PostgreSQL int64 Datum

The function follows PostgreSQL's function call conventions using the `PG_FUNCTION_ARGS` framework and appropriate return macros.

## Parameters / Member Variables
- Function uses PostgreSQL's standard function argument system:
  - Argument 0: OID of the relation whose size is to be calculated

## Dependencies
- Functions called/Symbols referenced:
  - `[try_relation_open](../t/try_relation_open.md)`: Safely opens a relation by OID with specified lock mode
  - `[calculate_table_size](../c/calculate_table_size.md)`: Core function that computes the actual table size
  - `[relation_close](../r/relation_close.md)`: Closes the relation and releases the lock
  - `PG_RETURN_INT64`: PostgreSQL macro for returning int64 values from functions
- Called from (representative examples):
  - This is a top-level SQL function, typically called directly from SQL queries

## Notes and Other Information
- Returns size in bytes as PostgreSQL int64 type
- Accessible via SQL as `pg_table_size(relation_oid)` or `pg_table_size('table_name')`
- Uses AccessShareLock to ensure safe concurrent access during size calculation
- Returns NULL rather than raising an error for non-existent relations
- Part of PostgreSQL's standard set of object size functions
- The size includes main table data, TOAST tables, FSM, and VM, but excludes indexes
- Function signature follows PostgreSQL's C function interface standards