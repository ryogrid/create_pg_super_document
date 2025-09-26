# FreePageManagerGet

## Location
src/backend/utils/mmgr/freepage.c: 210 - 251

## Overview
Allocates a contiguous run of pages from the free page manager and performs necessary cleanup operations to maintain data structure consistency.

## Definition
```c
bool FreePageManagerGet(FreePageManager *fpm, Size npages, Size *first_page)
```

## Detailed Description
FreePageManagerGet is the main interface for allocating contiguous pages from a free page manager. It wraps the internal allocation logic with additional maintenance operations including B-tree cleanup and largest contiguous range updates.

The allocation process involves:
1. Calling the internal allocation routine to find and reserve pages
2. Running B-tree cleanup operations that may create new opportunities for larger contiguous ranges
3. Updating the largest contiguous pages tracking
4. Maintaining debug assertions for consistency checking

A notable aspect is that allocation can paradoxically create opportunities for larger contiguous ranges through cleanup operations. When keys are removed from the B-tree during allocation, it may enable previously blocked recycling operations that can merge separated ranges into larger contiguous blocks.

## Parameters / Member Variables
- `fpm`: Pointer to the FreePageManager structure
- `npages`: Number of contiguous pages requested for allocation  
- `first_page`: Output parameter that receives the first page number of the allocated range on success

## Dependencies
- Functions called/Symbols referenced:
  - FreePageManagerGetInternal (performs actual allocation)
  - FreePageBtreeCleanup (cleanup and range merging)
  - FreePageManagerUpdateLargest (updates largest contiguous range tracking)
  - sum_free_pages (debug assertion)
  - FreePageManagerLargestContiguous (debug assertion)
- Called from (representative examples):
  - dsm_create
  - dsa_allocate_extended
  - ensure_active_superblock
  - fpm_largest

## Notes and Other Information
- Returns true if allocation succeeded, false if insufficient contiguous space available
- The counterintuitive cleanup behavior where allocation can create larger ranges is documented as a known characteristic
- Debug builds include extensive consistency checking via FPM_EXTRA_ASSERTS
- The function maintains both free page counts and largest contiguous range tracking
- Cleanup operations during allocation help maintain optimal data structure organization