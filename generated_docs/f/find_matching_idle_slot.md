# find_matching_idle_slot

## Location
[src/fe_utils/parallel_slot.c:135-158](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/fe_utils/parallel_slot.c#L135-L158)

## Overview
A static function that searches through a parallel slot array to find an idle slot that is connected to a specific database or any database if no specific database is requested.

## Definition
```c
static int find_matching_idle_slot(const ParallelSlotArray *sa, const char *dbname)
```

## Detailed Description
This function iterates through all slots in a parallel slot array to locate an available slot that meets the specified criteria. A suitable slot must not be currently in use, must have an active database connection, and must be connected to the specified database (or any database if dbname is NULL). The function provides flexible slot selection by allowing either database-specific or database-agnostic matching. It returns the index of the first matching slot found, or -1 if no suitable slot is available.

## Parameters / Member Variables
- `sa`: A const pointer to the ParallelSlotArray containing all available slots to search
- `dbname`: A const char pointer specifying the target database name, or NULL to match any database

## Dependencies
- Functions called/Symbols referenced:
  - [ParallelSlotArray](../P/ParallelSlotArray.md) (struct type for the slot array)
  - [PQdb](../P/PQdb.md) (PostgreSQL libpq function to get database name from connection)
- Called from (representative examples):
  - [ParallelSlotsGetIdle](../P/ParallelSlotsGetIdle.md)

## Notes and Other Information
- This is a static function, only accessible within the parallel_slot.c file
- Returns -1 when no suitable slot is found, otherwise returns the zero-based index of the matching slot
- Performs linear search through the slot array, returning the first match found
- The function checks three conditions: slot not in use, connection exists, and database name matches (if specified)
- Part of PostgreSQL's frontend utility library for managing parallel database connections
- Used primarily for connection reuse optimization in parallel processing scenarios

## Simplified Source

```c
static int
find_matching_idle_slot(const ParallelSlotArray *sa, const char *dbname)
{
    for (int i = 0; i < sa->numslots; i++) {
        // Skip slots that are currently in use
        if (sa->slots[i].inUse)
            continue;

        // Skip slots without active connections
        if (sa->slots[i].connection == NULL)
            continue;

        // Check if database matches (or accept any database if dbname is NULL)
        if (dbname == NULL ||
            strcmp(PQdb(sa->slots[i].connection), dbname) == 0) {
            return i;  // Found a suitable slot
        }
    }

    return -1;  // No suitable slot found
}
```