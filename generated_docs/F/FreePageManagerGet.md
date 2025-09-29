# FreePageManagerGet

## Location
[src/backend/utils/mmgr/freepage.c:210-251](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/freepage.c#L210-L251)

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
  - [FreePageManagerGetInternal](FreePageManagerGetInternal.md) (performs actual allocation)
  - [FreePageBtreeCleanup](FreePageBtreeCleanup.md) (cleanup and range merging)
  - [FreePageManagerUpdateLargest](FreePageManagerUpdateLargest.md) (updates largest contiguous range tracking)
  - [sum_free_pages](../s/sum_free_pages.md) (debug assertion)
  - [FreePageManagerLargestContiguous](FreePageManagerLargestContiguous.md) (debug assertion)
- Called from (representative examples):
  - [dsm_create](../d/dsm_create.md)
  - [dsa_allocate_extended](../d/dsa_allocate_extended.md)
  - [ensure_active_superblock](../e/ensure_active_superblock.md)
  - fpm_largest

## Notes and Other Information
- Returns true if allocation succeeded, false if insufficient contiguous space available
- The counterintuitive cleanup behavior where allocation can create larger ranges is documented as a known characteristic
- Debug builds include extensive consistency checking via FPM_EXTRA_ASSERTS
- The function maintains both free page counts and largest contiguous range tracking
- Cleanup operations during allocation help maintain optimal data structure organization

## Simplified Source

```c
bool
FreePageManagerGet(FreePageManager *fpm, Size npages, Size *first_page)
{
    bool result;
    Size contiguous_pages;

    // Attempt to allocate the requested pages
    result = FreePageManagerGetInternal(fpm, npages, first_page);

    // Cleanup B-tree after allocation
    // Note: Allocation can actually create larger contiguous ranges
    // by enabling cleanup operations that merge separated ranges
    contiguous_pages = FreePageBtreeCleanup(fpm);
    if (fpm->contiguous_pages < contiguous_pages)
        fpm->contiguous_pages = contiguous_pages;

    // Update largest contiguous range if needed
    FreePageManagerUpdateLargest(fpm);

#ifdef FPM_EXTRA_ASSERTS
    // Debug: Update free page count and verify consistency
    if (result) {
        Assert(fpm->free_pages >= npages);
        fpm->free_pages -= npages;
    }
    Assert(fpm->free_pages == sum_free_pages(fpm));
    Assert(fpm->contiguous_pages == FreePageManagerLargestContiguous(fpm));
#endif

    return result;
}
```