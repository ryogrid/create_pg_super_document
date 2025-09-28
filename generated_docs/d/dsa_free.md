# dsa_free

## Location
[src/backend/utils/mmgr/dsa.c:826-941](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/dsa.c#L826-L941)

## Overview
Frees memory that was previously allocated with dsa_allocate or dsa_allocate_extended within a dynamic shared memory area.

## Definition

```c
void
dsa_free(dsa_area *area, dsa_pointer dp)
```
## Detailed Description
This function deallocates memory within a DSA area that was previously allocated using  or . The function handles two different types of allocations:

1. **Large allocations** (DSA_SCLASS_SPAN_LARGE): These are freed by returning pages directly to the free page manager and freeing the associated span object. This involves unlinking the span from its pool and recursively calling  on the span pointer itself.

2. **Regular allocations**: These are freed by adding the object back to the span's freelist and potentially moving the span to a different fullness class or destroying the entire superblock if it becomes completely empty.

The function maintains proper memory management by tracking object allocation state through span metadata, managing fullness classes to optimize allocation performance, and preventing memory fragmentation through intelligent superblock destruction policies.

## Parameters / Member Variables
- : Pointer to the DSA area containing the memory to be freed
- : The dsa_pointer representing the memory block to free (must be a valid pointer returned by a previous allocation)

## Dependencies
- Functions called/Symbols referenced:
  - 
  - , , 
  - 
  - , 
  - , 
  - 
  - , 
  -  (recursive call for span objects)
- Called from:
  - Various executor functions (, , etc.)
  - [Hash](../H/Hash.md) table operations (, etc.)
  - , 
  - , 
  - 
  - Radix tree operations (, , etc.)
  -  (on error cleanup)

## Notes and Other Information
- The function automatically detects whether the memory being freed is a large allocation or regular allocation
- For debugging builds with CLOBBER_FREED_MEMORY defined, freed memory is overwritten with 0x7f bytes
- Thread-safe through appropriate locking mechanisms using DSA area locks and size class locks
- Implements intelligent superblock management to prevent memory fragmentation and hysteresis
- The active span in fullness class 1 is preserved even when empty to avoid allocation/deallocation thrashing
- Large object spans are handled specially with direct page manager interaction
- Recursive freeing is used for large allocation span objects

## Simplified Source

```c
// Simplified version of dsa_free
void dsa_free(dsa_area *area, dsa_pointer dp)
{
    dsa_segment_map *segment_map;
    int pageno;
    dsa_pointer span_pointer;
    dsa_area_span *span;
    char *object;
    size_t size;
    int size_class;

    // Ensure no stale segments exist
    check_for_freed_segments(area);

    // Locate the object and its containing span
    segment_map = get_segment_by_index(area, DSA_EXTRACT_SEGMENT_NUMBER(dp));
    pageno = DSA_EXTRACT_OFFSET(dp) / FPM_PAGE_SIZE;
    span_pointer = segment_map->pagemap[pageno];
    span = dsa_get_address(area, span_pointer);
    object = dsa_get_address(area, dp);
    size_class = span->size_class;
    size = dsa_size_classes[size_class];

    // Handle large objects specially
    if (span->size_class == DSA_SCLASS_SPAN_LARGE) {
        // Return pages directly to free page manager
        LWLockAcquire(DSA_AREA_LOCK(area), LW_EXCLUSIVE);
        FreePageManagerPut(segment_map->fpm,
                          DSA_EXTRACT_OFFSET(span->start) / FPM_PAGE_SIZE,
                          span->npages);
        rebin_segment(area, segment_map);
        LWLockRelease(DSA_AREA_LOCK(area));

        // Unlink and free the span
        LWLockAcquire(DSA_SCLASS_LOCK(area, DSA_SCLASS_SPAN_LARGE), LW_EXCLUSIVE);
        unlink_span(area, span);
        LWLockRelease(DSA_SCLASS_LOCK(area, DSA_SCLASS_SPAN_LARGE));

        // Recursively free the span object
        dsa_free(area, span_pointer);
        return;
    }

    // Handle regular objects
    LWLockAcquire(DSA_SCLASS_LOCK(area, size_class), LW_EXCLUSIVE);

    // Add object back to span's freelist
    NextFreeObjectIndex(object) = span->firstfree;
    span->firstfree = (object - superblock) / size;
    ++span->nallocatable;

    // Move span to appropriate fullness class if needed
    if (span->nallocatable == 1 && span->fclass == DSA_FULLNESS_CLASSES - 1) {
        // Move from completely full class to next lower class
        unlink_span(area, span);
        add_span_to_fullness_class(area, span, span_pointer, DSA_FULLNESS_CLASSES - 2);
    }
    else if (span->nallocatable == span->nmax &&
             (span->fclass != 1 || span->prevspan != InvalidDsaPointer)) {
        // Span is completely empty and not the active span - destroy it
        destroy_superblock(area, span_pointer);
    }

    LWLockRelease(DSA_SCLASS_LOCK(area, size_class));
}
```

Key simplifications made:
- Removed debug memory clobbering code (CLOBBER_FREED_MEMORY sections)
- Simplified variable declarations by removing some intermediate variables
- Removed detailed assertions and safety checks for clarity
- Consolidated comments to focus on main logic flow
- Abstracted complex pointer arithmetic and offset calculations
- Removed detailed comments about hysteresis prevention while preserving the logic
- Streamlined the fullness class management logic presentation