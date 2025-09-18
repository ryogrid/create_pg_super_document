# FreePageManagerPut

## Location
src/backend/utils/mmgr/freepage.c: 379 - 423

## Overview
Transfers a contiguous run of pages to the free page manager, making them available for future allocation.

## Definition


## Detailed Description
This function adds a contiguous range of pages to the free page manager's internal data structures. It handles the complex task of inserting the new pages while maintaining optimal organization of free space and potentially consolidating adjacent free ranges.

The function performs several key operations:
1. Records the new pages using 
2. If the new range merged with existing ranges, it may trigger cleanup operations via  to optimize the internal B-tree structure
3. Updates the cached largest contiguous chunk size if the newly available space is larger
4. Ensures the largest contiguous pages cache is current by calling 

The function includes debug assertions to verify data structure consistency when  is enabled.

## Parameters / Member Variables
- : Pointer to the FreePageManager structure to add pages to
- : The starting page number of the range to be freed
- : The number of contiguous pages to add to the free pool (must be > 0)

## Dependencies
- Functions called/Symbols referenced:
  - FreePageManagerPutInternal
  - FreePageBtreeCleanup
  - FreePageManagerUpdateLargest
  - sum_free_pages (debug only)
  - FreePageManagerLargestContiguous (debug only)
- Called from (representative examples):
  - dsm_shmem_init
  - dsm_create
  - dsm_detach
  - dsa_free
  - make_new_segment

## Notes and Other Information
This is a public API function (non-static) used by dynamic shared memory (DSM) and dynamic shared arrays (DSA) subsystems. The function handles memory coalescing optimization and maintains internal consistency. Debug builds include additional assertions to verify the correctness of free page accounting and contiguous page calculations.