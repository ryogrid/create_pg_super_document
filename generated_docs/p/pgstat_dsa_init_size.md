# pgstat_dsa_init_size

## Location
[src/backend/utils/activity/pgstat_shmem.c:106-126](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_shmem.c#L106-L126)

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
  - [dsa_minimum_size](../d/dsa_minimum_size.md): Validates that the allocated size meets minimum DSA requirements
  - MAXALIGN: Ensures proper memory alignment
- Called from (representative examples):
  - [StatsShmemSize](../S/StatsShmemSize.md): Uses this size in total shared memory calculation
  - [StatsShmemInit](../S/StatsShmemInit.md): Uses this size for DSA initialization

## Notes and Other Information
- Fixed allocation size of 256KB (256 * 1024 bytes) provides good balance between memory usage and performance
- The size is chosen to avoid immediate need for dynamic shared memory segments
- Users can configure min_dynamic_shared_memory to further avoid DSM usage
- The allocation is MAXALIGN'd to ensure proper memory alignment
- Contains assertion to verify the size meets dsa_minimum_size() requirements

## Simplified Source

```c
// Simplified version of pgstat_dsa_init_size
static Size pgstat_dsa_init_size(void) {
    Size sz;

    // Fixed allocation of 256KB for shared stats hash table
    // Large enough for dshash header and initial buckets without requiring DSM
    sz = 256 * 1024;

    // Verify this meets minimum DSA requirements
    Assert(dsa_minimum_size() <= sz);

    // Return with proper memory alignment
    return MAXALIGN(sz);
}
```

Key simplifications made:
- Added clear comments explaining the 256KB allocation rationale
- Preserved essential validation through dsa_minimum_size() assertion
- Maintained proper memory alignment
- Focused on the core purpose: providing optimal initial DSA size
- Kept the balance between avoiding DSM segments and reasonable memory usage