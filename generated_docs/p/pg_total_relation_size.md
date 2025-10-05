# pg_total_relation_size

## Location
[src/backend/utils/adt/dbsize.c:547-568](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/dbsize.c#L547-L568)

## Overview
This function calculates and returns the total size of a PostgreSQL relation (table/index) including all associated files such as TOAST tables, indexes, and FSM/VM files.

## Definition

```c
struct size_pretty_unit *unit;
```
## Detailed Description
The pg_total_relation_size function is a PostgreSQL built-in function that computes the complete disk space usage of a relation. Unlike pg_relation_size which only returns the size of the main relation fork, this function includes all associated storage:
- Main relation data
- TOAST table data (if any)
- All indexes on the relation
- Free Space Map (FSM) and Visibility Map (VM) files

The function safely handles relations that may not exist or be accessible by returning NULL if the relation cannot be opened with AccessShareLock.

## Parameters / Member Variables
- : The OID (Object Identifier) of the relation whose total size is to be calculated

## Dependencies
- Functions called/Symbols referenced:
  - [try_relation_open](../t/try_relation_open.md) (safely opens relation with lock)
  - [calculate_total_relation_size](../c/calculate_total_relation_size.md) (performs the actual size calculation)
  - [relation_close](../r/relation_close.md) (closes relation and releases lock)
  - PG_RETURN_INT64 (returns 64-bit integer result)
- Called from (representative examples):
  - SQL queries using pg_total_relation_size() function
  - System catalog queries for disk space monitoring

## Notes and Other Information
- Returns NULL if the relation OID is invalid or inaccessible
- Uses AccessShareLock to ensure consistent size calculation
- [Result](../R/Result.md) is in bytes as a 64-bit integer
- This is the SQL-callable version of the total relation size calculation
- Commonly used in database administration for monitoring disk usage

## Simplified Source

```c
Datum
pg_total_relation_size(PG_FUNCTION_ARGS)
{
    Oid relOid = PG_GETARG_OID(0);
    Relation rel;
    int64 size;

    // Try to open the relation safely
    rel = try_relation_open(relOid, AccessShareLock);

    if (rel == NULL)
        PG_RETURN_NULL();

    // Calculate total relation size (table + indexes + TOAST)
    size = calculate_total_relation_size(rel);

    relation_close(rel, AccessShareLock);

    PG_RETURN_INT64(size);
}
```