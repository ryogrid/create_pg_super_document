# pgstat_get_entry_data

## Location
src/include/utils/pgstat_internal.h: 818 - 827

## Overview
A static inline function that returns a pointer to the data portion of a shared memory statistics entry, calculating the appropriate offset based on the statistics kind.

## Definition
```c
static inline void *
pgstat_get_entry_data(PgStat_Kind kind, PgStatShared_Common *entry)
```

## Detailed Description
This function calculates and returns a pointer to the actual data portion of a shared memory statistics entry. It retrieves the shared_data_off offset from the kind-specific information and applies it to the provided entry pointer to locate the data section. The function performs bounds checking to ensure the offset is valid and within acceptable limits, then returns a properly positioned pointer for accessing the statistics data.

## Parameters / Member Variables
- `kind`: The PgStat_Kind enum value specifying the type of statistics entry
- `entry`: Pointer to the PgStatShared_Common base structure of the statistics entry

## Dependencies
- Functions called/Symbols referenced:
  - PgStat_Kind (enum type for statistics kinds)
  - PgStatShared_Common (base structure for shared statistics entries)
  - pgstat_get_kind_info (function to get kind-specific information)
  - PG_UINT32_MAX (constant for maximum 32-bit unsigned integer value)
  - Assert (macro for debug assertions)
- Called from (representative examples):
  - pgstat_fetch_entry in pgstat.c
  - pgstat_build_snapshot in pgstat.c
  - pgstat_write_statsfile in pgstat.c
  - pgstat_read_statsfile in pgstat.c
  - pgstat_reinit_entry in pgstat_shmem.c
  - shared_stat_reset_contents in pgstat_shmem.c

## Notes and Other Information
- Performs bounds checking with assertions to ensure the offset is non-zero and within valid range
- Returns a void pointer that typically needs to be cast to the appropriate statistics data structure type
- The offset calculation allows for different statistics kinds to have different data layouts while sharing a common base structure
- Designed as a static inline function for performance optimization in frequent statistics access operations
- Essential for proper access to type-specific statistics data in shared memory
- Part of the PostgreSQL statistics collection infrastructure for managing heterogeneous statistics entry types