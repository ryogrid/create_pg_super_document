# dsa_minimum_size

## Location
src/backend/utils/mmgr/dsa.c: 1196 - 1217

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
None - this is a parameter-less function that returns a computed value.

## Dependencies
- Functions called/Symbols referenced:
  - MAXALIGN
  - FPM_PAGE_SIZE
- Called from (representative examples):
  - ExecInitParallelPlan (in execParallel.c)
  - pgstat_dsa_init_size (in pgstat_shmem.c)
  - create_internal (in dsa.c)

## Notes and Other Information
- Uses MAXALIGN to ensure proper structure alignment in memory
- Accounts for the circular dependency between total size and page map size through iteration
- The returned size is always a multiple of FPM_PAGE_SIZE
- Essential for determining buffer sizes when using dsa_create_in_place
- Used internally by DSA creation functions to validate minimum size requirements
- The calculation ensures all essential DSA structures can fit within the allocated space