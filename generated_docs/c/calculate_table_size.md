# calculate_table_size

## Location
[src/backend/utils/adt/dbsize.c:424-450](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/dbsize.c#L424-L450)

## Overview
Calculates the total on-disk size of a given table, including all forks (FSM, VM) and the associated TOAST table if any, but excluding indexes other than the TOAST table's index.

## Definition

```c
static int64
calculate_table_size(Relation rel)
```
## Detailed Description
This function computes the complete storage footprint of a PostgreSQL table by examining all storage components:

1. **Main table forks**: Iterates through all fork numbers (0 to MAX_FORKNUM) to include the main heap, Free Space Map (FSM), and Visibility Map (VM)
2. **TOAST table**: If the relation has an associated TOAST table (for storing large attribute values), includes its complete size
3. **Index exclusion**: Deliberately excludes regular indexes from the calculation, focusing only on the table's own storage

The function works correctly when applied to indexes or TOAST tables themselves, treating them as regular relations without attached TOAST tables.

## Parameters / Member Variables
- `rel`: Relation pointer to the table whose size is being calculated
## Dependencies
- Functions called/Symbols referenced:
  - : Maximum fork number constant for iterating through all forks
  - : Calculates size of a specific relation fork
  - : Calculates total size of the TOAST table
- Called from (representative examples):
  - : SQL function wrapper for table size calculation
  - : Used in total relation size calculation

## Notes and Other Information
- Returns size in bytes as int64
- The function is static, meaning it's only accessible within the dbsize.c compilation unit
- Handles both regular tables and special cases (indexes, TOAST tables) gracefully
- TOAST table size calculation is conditional on the existence of a valid TOAST relation OID
- This is a core utility function for PostgreSQL's size reporting system functions

## Simplified Source

```c
static int64
calculate_table_size(Relation rel)
{
    int64 size = 0;
    ForkNumber forkNum;

    // Calculate size of all table forks (main heap, FSM, VM)
    for (forkNum = 0; forkNum <= MAX_FORKNUM; forkNum++)
        size += calculate_relation_size(&(rel->rd_locator), rel->rd_backend, forkNum);

    // Add TOAST table size if present
    if (OidIsValid(rel->rd_rel->reltoastrelid))
        size += calculate_toast_table_size(rel->rd_rel->reltoastrelid);

    return size;
}
```