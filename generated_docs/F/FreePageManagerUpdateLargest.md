# FreePageManagerUpdateLargest

## Location
[src/backend/utils/mmgr/freepage.c:366-378](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/freepage.c#L366-L378)

## Overview
Updates the cached size of the largest contiguous run of free pages when the cache has been marked dirty.

## Definition

```c
static void
FreePageManagerUpdateLargest(FreePageManager *fpm)
```
## Detailed Description
This function is a lazy cache update mechanism for tracking the largest contiguous block of free pages available in a FreePageManager. The function only performs work when the  flag is set, indicating that the cached value may be stale due to recent page allocations or deallocations.

When called, it recomputes the actual largest contiguous page run by calling  and updates the cached value in . After the update, it clears the dirty flag to indicate the cache is now current.

## Parameters / Member Variables
- : Pointer to the FreePageManager structure whose largest contiguous pages cache needs updating

## Dependencies
- Functions called/Symbols referenced:
  - [FreePageManager](FreePageManager.md) (struct type)
  - [FreePageManagerLargestContiguous](FreePageManagerLargestContiguous.md)
- Called from (representative examples):
  - [FreePageManagerGet](FreePageManagerGet.md)
  - [FreePageManagerPut](FreePageManagerPut.md)

## Notes and Other Information
This is an internal static function that implements a lazy evaluation pattern for performance optimization. Rather than recalculating the largest contiguous block size on every page operation, the function only updates when necessary, reducing computational overhead in scenarios with frequent page management operations.

## Simplified Source

```c
// Simplified version of FreePageManagerUpdateLargest
static void
FreePageManagerUpdateLargest(FreePageManager *fpm)
{
    // Only update if the cache is marked as dirty
    if (!fpm->contiguous_pages_dirty)
        return;

    // Recompute the largest contiguous page run
    fpm->contiguous_pages = FreePageManagerLargestContiguous(fpm);

    // Mark cache as clean
    fpm->contiguous_pages_dirty = false;
}
```

Key simplifications made:
- Added explanatory comments for each logical step
- Function is already quite simple and concise - no major simplifications needed
- Preserved the lazy evaluation pattern which is the core logic