# pgstat_get_entry_len

## Location
[src/include/utils/pgstat_internal.h:809-817](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/pgstat_internal.h#L809-L817)

## Overview
A static inline function that returns the size of the data portion of a shared memory statistics entry, excluding transient data like reference counts and locks.

## Definition
```c
static inline size_t
pgstat_get_entry_len(PgStat_Kind kind)
```

## Detailed Description
This function retrieves the length of the shared data portion of a statistics entry based on the statistics kind. It serves as a convenience wrapper around pgstat_get_kind_info() to access the shared_data_len field. The returned size represents only the persistent data that needs to be stored in shared memory, excluding transient metadata such as reference counts, lwlocks, and other runtime management data.

## Parameters / Member Variables
- `kind`: The PgStat_Kind enum value specifying the type of statistics entry

## Dependencies
- Functions called/Symbols referenced:
  - [PgStat_Kind](../P/PgStat_Kind.md) (enum type for statistics kinds)
  - [pgstat_get_kind_info](pgstat_get_kind_info.md) (function to get kind-specific information)
- Called from (representative examples):
  - [pgstat_write_statsfile](pgstat_write_statsfile.md) in pgstat.c
  - [pgstat_read_statsfile](pgstat_read_statsfile.md) in pgstat.c
  - [pgstat_reinit_entry](pgstat_reinit_entry.md) in pgstat_shmem.c
  - [shared_stat_reset_contents](../s/shared_stat_reset_contents.md) in pgstat_shmem.c

## Notes and Other Information
- The function returns the size in bytes of the shared data portion only
- Used primarily in statistics file I/O operations and shared memory management
- Excludes transient data such as reference counts and lwlocks from the size calculation
- Designed as a static inline function for performance optimization
- Essential for proper memory allocation and serialization of statistics data
- Part of the PostgreSQL statistics collection infrastructure for managing different types of statistical entries

## Simplified Source

```c
// Simplified version of pgstat_get_entry_len
static inline size_t pgstat_get_entry_len(PgStat_Kind kind) {
    // Return the data length for this statistics kind
    return pgstat_get_kind_info(kind)->shared_data_len;
}
```

Key simplifications made:
- Simple one-line function with minimal complexity
- Core functionality preserved: retrieves shared data length from kind info
- Essential size calculation maintained for statistics data management