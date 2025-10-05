# calculate_total_relation_size

## Location
[src/backend/utils/adt/dbsize.c:528-546](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/dbsize.c#L528-L546)

## Overview
Computes the complete on-disk size of a relation including all components: heap data, index data, TOAST data, FSM, and VM.

## Definition
```c
static int64 calculate_total_relation_size(Relation rel)
```

## Detailed Description
This function provides a comprehensive size calculation for PostgreSQL relations by aggregating all storage components associated with a table. It serves as a high-level coordinator that combines the results of specialized size calculation functions:

1. **Table size calculation**: Calls `calculate_table_size` to get the size of the main table, including:
   - Heap data (main table storage)
   - TOAST table and its index
   - Free Space Map (FSM)
   - Visibility Map (VM)

2. **Index size calculation**: Calls `calculate_indexes_size` to get the total size of all indexes attached to the table (excluding the TOAST index which is already counted in table size)

3. **Total aggregation**: Simply adds the table size and indexes size to provide the complete storage footprint

This function represents the most comprehensive size measurement available for PostgreSQL relations.

## Parameters / Member Variables
- `rel`: Relation pointer to the table whose total size is being calculated

## Dependencies
- Functions called/Symbols referenced:
  - [calculate_table_size](calculate_table_size.md): Calculates size of table including TOAST, FSM, VM
  - [calculate_indexes_size](calculate_indexes_size.md): Calculates total size of all attached indexes
- Called from (representative examples):
  - [pg_total_relation_size](../p/pg_total_relation_size.md): SQL function wrapper for total relation size calculation

## Notes and Other Information
- Returns total size in bytes as int64
- The function is static, limiting its scope to the dbsize.c compilation unit
- Provides the most complete size measurement by including all relation components
- Used as the basis for PostgreSQL's `pg_total_relation_size()` SQL function
- Efficiently reuses existing specialized calculation functions rather than duplicating logic
- The calculation is comprehensive but excludes no relation components
- Represents the actual disk space consumed by a complete table and all its associated objects

## Simplified Source

```c
static int64
calculate_total_relation_size(Relation rel)
{
    int64 size;

    // Get table size (heap + TOAST + FSM + VM)
    size = calculate_table_size(rel);

    // Add all indexes size
    size += calculate_indexes_size(rel);

    return size;
}
```