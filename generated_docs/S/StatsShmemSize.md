# StatsShmemSize

## Location
[src/backend/utils/activity/pgstat_shmem.c:127-140](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_shmem.c#L127-L140)

## Overview
Function that computes the total shared memory space needed for PostgreSQL's cumulative statistics system.

## Definition

```c
Size
StatsShmemSize(void)
```
## Detailed Description
This function calculates the total amount of shared memory required for the PostgreSQL statistics system. It combines the size of the main statistics shared memory control structure (PgStat_ShmemControl) with the dynamic shared memory area size needed for the statistics hash table. The calculation ensures proper memory alignment and uses safe size addition to prevent overflow.

## Parameters / Member Variables
- No parameters (void function)
- Returns: Size value representing total bytes needed for statistics shared memory

## Dependencies
- Functions called/Symbols referenced:
  - MAXALIGN: Ensures proper alignment of PgStat_ShmemControl size
  - add_size: Safely adds sizes to prevent overflow
  - pgstat_dsa_init_size: Gets the DSA initialization size (256KB)
  - PgStat_ShmemControl: Main statistics control structure
- Called from (representative examples):
  - CalculateShmemSize: Uses this in total shared memory calculations during startup
  - StatsShmemInit: Verifies size during initialization

## Notes and Other Information
- Part of the shared memory sizing calculation performed during PostgreSQL startup
- Ensures proper alignment of all statistics-related shared memory structures
- Uses safe arithmetic operations to prevent integer overflow
- The total size includes both the control structure and the dynamic hash table area