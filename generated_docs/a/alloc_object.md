# alloc_object

## Location
[src/backend/utils/mmgr/dsa.c:1472-1559](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/dsa.c#L1472-L1559)

## Overview
Allocates a single object of the specified size class from a DSA area, managing span utilization and free object tracking within the dynamic shared memory allocation system.

## Definition

```c
static inline dsa_pointer
alloc_object(dsa_area *area, int size_class)
```
## Detailed Description
This function implements the core object allocation logic for the Dynamic Shared Area (DSA) memory management system. It operates by acquiring the appropriate size class lock, ensuring an active superblock is available for allocation, and then either reusing a previously freed object from the free list or initializing a new object within the span.

The function maintains efficient allocation patterns by preferring to allocate from spans in fullness class 1 (partially full) and automatically moving spans to the highest fullness class when they become completely full. It handles both recycled objects (from the free list) and fresh object initialization, updating span metadata to track allocation state.

## Parameters / Member Variables
- : Pointer to the DSA area from which to allocate
- : Index identifying the size class of objects to allocate

## Dependencies
- Functions called/Symbols referenced:
  - [LWLockHeldByMe](../L/LWLockHeldByMe.md)
  - [LWLockAcquire](../L/LWLockAcquire.md)
  - [LWLockRelease](../L/LWLockRelease.md)
  - DSA_SCLASS_LOCK
  - DsaPointerIsValid
  - [ensure_active_superblock](../e/ensure_active_superblock.md)
  - [dsa_get_address](../d/dsa_get_address.md)
  - NextFreeObjectIndex
  - [transfer_first_span](../t/transfer_first_span.md)
  - dsa_size_classes array
  - DSA_SPAN_NOTHING_FREE
  - DSA_FULLNESS_CLASSES
  - DSA_NUM_SIZE_CLASSES
  - InvalidDsaPointer
- Called from (representative examples):
  - [dsa_allocate_extended](../d/dsa_allocate_extended.md)
  - [ensure_active_superblock](../e/ensure_active_superblock.md)

## Notes and Other Information
- This is a static inline function optimized for frequent allocation operations
- Acquires exclusive lock on the size class to ensure thread-safe allocation
- Implements a two-tier allocation strategy: free list recycling and fresh object initialization
- Automatically manages span fullness class transitions to maintain allocation efficiency
- Returns InvalidDsaPointer if no memory is available for allocation
- Critical path function that must maintain high performance for DSA allocation operations
- The function can recursively call ensure_active_superblock, which may in turn call alloc_object for different size classes (safe due to lock ordering)
- Maintains span metadata including nallocatable, ninitialized, and firstfree counters
- Essential component of PostgreSQL's shared memory object allocation infrastructure

## Simplified Source

```c
static inline dsa_pointer alloc_object(dsa_area *area, int size_class)
{
    dsa_area_pool *pool = &area->control->pools[size_class];
    dsa_area_span *span;
    dsa_pointer block;
    dsa_pointer result;
    char *object;
    size_t size;

    // Acquire exclusive lock for this size class
    Assert(!LWLockHeldByMe(DSA_SCLASS_LOCK(area, size_class)));
    LWLockAcquire(DSA_SCLASS_LOCK(area, size_class), LW_EXCLUSIVE);

    // Ensure we have an active superblock to allocate from
    if (!DsaPointerIsValid(pool->spans[1]) &&
        !ensure_active_superblock(area, pool, size_class)) {
        result = InvalidDsaPointer;
    } else {
        // Get the active span (should be in fullness class 1)
        Assert(DsaPointerIsValid(pool->spans[1]));
        span = (dsa_area_span *) dsa_get_address(area, pool->spans[1]);
        Assert(span->nallocatable > 0);

        block = span->start;
        size = dsa_size_classes[size_class];

        // Try to reuse a freed object first
        if (span->firstfree != DSA_SPAN_NOTHING_FREE) {
            result = block + span->firstfree * size;
            object = dsa_get_address(area, result);
            span->firstfree = NextFreeObjectIndex(object);
        } else {
            // Initialize a new object
            result = block + span->ninitialized * size;
            ++span->ninitialized;
        }

        // Update availability count
        --span->nallocatable;

        // Move span to full class if it's now completely full
        if (span->nallocatable == 0)
            transfer_first_span(area, pool, 1, DSA_FULLNESS_CLASSES - 1);
    }

    LWLockRelease(DSA_SCLASS_LOCK(area, size_class));
    return result;
}
```