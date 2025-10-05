# pg_indexes_size

## Location
[src/backend/utils/adt/dbsize.c:505-527](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/dbsize.c#L505-L527)

## Overview
SQL-callable function that returns the total disk space used by all indexes on the specified table, including all index storage components.

## Definition
```c
Datum pg_indexes_size(PG_FUNCTION_ARGS)
```

## Detailed Description
This function provides the SQL interface for PostgreSQL's `pg_indexes_size()` system function, which calculates the aggregate storage size of all indexes associated with a table. It follows the same pattern as other PostgreSQL size functions:

1. **Input processing**: Extracts the relation OID from function arguments
2. **Safe relation access**: Opens the relation using `try_relation_open` with AccessShareLock
3. **Error handling**: Returns NULL if the relation cannot be accessed (non-existent, insufficient permissions)
4. **Index size calculation**: Delegates to `calculate_indexes_size` for the actual computation
5. **Resource management**: Properly closes the relation and releases locks
6. **Result return**: Returns the total size as a PostgreSQL int64 Datum

The function is designed to be safe for concurrent use and handles edge cases gracefully.

## Parameters / Member Variables
- Function uses PostgreSQL's standard function argument system:
  - Argument 0: OID of the relation whose indexes' sizes are to be calculated

## Dependencies
- Functions called/Symbols referenced:
  - [try_relation_open](../t/try_relation_open.md): Safely opens a relation by OID with specified lock mode
  - [calculate_indexes_size](../c/calculate_indexes_size.md): Core function that computes the total indexes size
  - [relation_close](../r/relation_close.md): Closes the relation and releases the lock
  - `PG_RETURN_INT64`: PostgreSQL macro for returning int64 values from functions
- Called from (representative examples):
  - This is a top-level SQL function, typically called directly from SQL queries

## Notes and Other Information
- Returns total size in bytes as PostgreSQL int64 type
- Accessible via SQL as `pg_indexes_size(relation_oid)` or `pg_indexes_size('table_name')`
- Uses AccessShareLock for safe concurrent access during index size calculation
- Returns NULL instead of raising errors for inaccessible relations
- Returns 0 when applied to relations without indexes or to index relations themselves
- Includes all index storage components (main data, FSM, VM) for comprehensive reporting
- Part of PostgreSQL's standard object size function family
- Function follows PostgreSQL's C function interface conventions

## Simplified Source

```c
Datum
pg_indexes_size(PG_FUNCTION_ARGS)
{
    Oid relOid = PG_GETARG_OID(0);
    Relation rel;
    int64 size;

    // Try to open the relation safely
    rel = try_relation_open(relOid, AccessShareLock);

    if (rel == NULL)
        PG_RETURN_NULL();

    // Calculate indexes size using core function
    size = calculate_indexes_size(rel);

    relation_close(rel, AccessShareLock);

    PG_RETURN_INT64(size);
}
```