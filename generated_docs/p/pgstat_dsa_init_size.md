# pgstat_dsa_init_size

## Location
src/backend/utils/activity/pgstat_shmem.c: 106 - 126

## Overview
Static function that determines the size of shared memory allocation for the dynamic shared hash table used by PostgreSQL's statistics system.

## Definition

```c
static Size
pgstat_dsa_init_size(void)
```
## Detailed Description
This function calculates the initial size allocation for the dynamic shared memory area (DSA) used by PostgreSQL's statistics shared hash table. The allocation is done as part of the main shared memory rather than dynamic shared memory, allowing it to be initialized in the postmaster process. The function returns a fixed size of 256KB, which provides enough space for the dshash header and initial bucket arrays without requiring immediate dynamic shared memory segments.

## Parameters / Member Variables
- No parameters (void function)

## Dependencies
- Functions called/Symbols referenced:
  - dsa_minimum_size: Validates that the allocated size meets minimum DSA requirements
  - MAXALIGN: Ensures proper memory alignment
- Called from (representative examples):
  - StatsShmemSize: Uses this size in total shared memory calculation
  - StatsShmemInit: Uses this size for DSA initialization

## Notes and Other Information
- Fixed allocation size of 256KB (256 * 1024 bytes) provides good balance between memory usage and performance
- The size is chosen to avoid immediate need for dynamic shared memory segments
- Users can configure min_dynamic_shared_memory to further avoid DSM usage
- The allocation is MAXALIGN'd to ensure proper memory alignment
- Contains assertion to verify the size meets dsa_minimum_size() requirements