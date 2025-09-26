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