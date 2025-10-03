# dsa_minimum_size

## Location
[src/backend/utils/mmgr/dsa.c:1196-1217](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/dsa.c#L1196-L1217)

## Overview
Returns the smallest size in bytes that can be successfully provided to dsa_create_in_place when creating a new dynamic shared memory area.

## Definition
```c
size_t dsa_minimum_size(void)
```

## Detailed Description
This function calculates the minimum memory requirement needed to initialize a DSA area in a pre-allocated memory region. The calculation includes space for essential control structures and accounts for the circular dependency between the total size and the page map size.

The minimum size includes:
- Space for the dsa_area_control structure (properly aligned)
- Space for the FreePageManager structure (properly aligned)  
- Space for the page map, where each page requires a dsa_pointer entry

The function uses an iterative approach to resolve the circular dependency: as more pages are needed to accommodate the growing page map, additional dsa_pointer entries must be allocated, which may require even more pages. The loop continues until the calculation converges on a stable value.

## Parameters / Member Variables

## Dependencies
- Functions called/Symbols referenced:
  - MAXALIGN
  - FPM_PAGE_SIZE
- Called from (representative examples):
  - [ExecInitParallelPlan](../E/ExecInitParallelPlan.md) (in execParallel.c)
  - [pgstat_dsa_init_size](../p/pgstat_dsa_init_size.md) (in pgstat_shmem.c)
  - [create_internal](../c/create_internal.md) (in dsa.c)

## Notes and Other Information
- Uses MAXALIGN to ensure proper structure alignment in memory
- Accounts for the circular dependency between total size and page map size through iteration
- The returned size is always a multiple of FPM_PAGE_SIZE
- Essential for determining buffer sizes when using dsa_create_in_place
- Used internally by DSA creation functions to validate minimum size requirements
- The calculation ensures all essential DSA structures can fit within the allocated space

## Simplified Source

```c
// Simplified version of dsa_minimum_size
size_t dsa_minimum_size(void) {
    size_t size;
    int pages = 0;

    // Start with basic control structures
    size = MAXALIGN(sizeof(dsa_area_control)) +
           MAXALIGN(sizeof(FreePageManager));

    // Iteratively calculate pages needed, accounting for page map growth
    while (((size + FPM_PAGE_SIZE - 1) / FPM_PAGE_SIZE) > pages) {
        pages++;
        size += sizeof(dsa_pointer);  // Each page needs a pointer in the map
    }

    // Return total size as multiple of page size
    return pages * FPM_PAGE_SIZE;
}
```

Key simplifications made:
- Added descriptive comments explaining the logic flow
- Clarified the iterative calculation approach
- Explained the circular dependency resolution between size and page count
- Made variable purposes clearer with inline comments