# FreePageManagerPut

## Location
[src/backend/utils/mmgr/freepage.c:379-423](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/freepage.c#L379-L423)

## Overview
Transfers a contiguous run of pages to the free page manager, making them available for future allocation.

## Definition

```c
void
FreePageManagerPut(FreePageManager *fpm, Size first_page, Size npages)
```
## Detailed Description
This function adds a contiguous range of pages to the free page manager's internal data structures. It handles the complex task of inserting the new pages while maintaining optimal organization of free space and potentially consolidating adjacent free ranges.

The function performs several key operations:
1. Records the new pages using 
2. If the new range merged with existing ranges, it may trigger cleanup operations via  to optimize the internal B-tree structure
3. Updates the cached largest contiguous chunk size if the newly available space is larger
4. Ensures the largest contiguous pages cache is current by calling 

The function includes debug assertions to verify data structure consistency when  is enabled.

## Parameters / Member Variables
- `*fpm`: Pointer to the FreePageManager structure to add pages to
- `first_page`: The starting page number of the range to be freed
- `npages`: The number of contiguous pages to add to the free pool (must be > 0)
## Dependencies
- Functions called/Symbols referenced:
  - [FreePageManagerPutInternal](FreePageManagerPutInternal.md)
  - [FreePageBtreeCleanup](FreePageBtreeCleanup.md)
  - [FreePageManagerUpdateLargest](FreePageManagerUpdateLargest.md)
  - [sum_free_pages](../s/sum_free_pages.md) (debug only)
  - [FreePageManagerLargestContiguous](FreePageManagerLargestContiguous.md) (debug only)
- Called from (representative examples):
  - [dsm_shmem_init](../d/dsm_shmem_init.md)
  - [dsm_create](../d/dsm_create.md)
  - [dsm_detach](../d/dsm_detach.md)
  - [dsa_free](../d/dsa_free.md)
  - [make_new_segment](../m/make_new_segment.md)

## Notes and Other Information
This is a public API function (non-static) used by dynamic shared memory (DSM) and dynamic shared arrays (DSA) subsystems. The function handles memory coalescing optimization and maintains internal consistency. Debug builds include additional assertions to verify the correctness of free page accounting and contiguous page calculations.

## Simplified Source

```c
// Simplified version of FreePageManagerPut
void FreePageManagerPut(FreePageManager *fpm, Size first_page, Size npages) {
    Size contiguous_pages;

    Assert(npages > 0);

    // Add the new pages to internal data structures
    contiguous_pages = FreePageManagerPutInternal(fpm, first_page, npages, false);

    // If pages merged with existing ranges, try cleanup optimizations
    if (contiguous_pages > npages) {
        Size cleanup_contiguous_pages;

        cleanup_contiguous_pages = FreePageBtreeCleanup(fpm);
        if (cleanup_contiguous_pages > contiguous_pages)
            contiguous_pages = cleanup_contiguous_pages;
    }

    // Update largest chunk tracking if we have a bigger chunk now
    if (fpm->contiguous_pages < contiguous_pages)
        fpm->contiguous_pages = contiguous_pages;

    // Ensure largest chunk cache is current (may have been marked dirty)
    FreePageManagerUpdateLargest(fpm);

#ifdef FPM_EXTRA_ASSERTS
    // Debug: verify accounting consistency
    fpm->free_pages += npages;
    Assert(fpm->free_pages == sum_free_pages(fpm));
    Assert(fpm->contiguous_pages == FreePageManagerLargestContiguous(fpm));
#endif
}
```

Key simplifications made:
- Added clear comments explaining each major operation phase
- Simplified conditional logic explanation with inline comments
- Maintained all original functionality including debug assertions
- Focused on the high-level flow: insert, optimize, update tracking