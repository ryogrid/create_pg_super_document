# dsa_allocate_extended

## Location
[src/backend/utils/mmgr/dsa.c:671-825](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/dsa.c#L671-L825)

## Overview
Allocates memory in a dynamic shared memory area and returns a portable dsa_pointer that can be shared across processes.

## Definition

```c
dsa_pointer
dsa_allocate_extended(dsa_area *area, size_t size, int flags)
```
## Detailed Description
This function allocates memory of the specified size within a dynamic shared memory (DSA) area. Unlike traditional memory allocation functions, it returns a  which is a portable reference that can be passed to other processes and converted to a local pointer using . The function supports various allocation strategies based on the provided flags:

For very large allocations (larger than the largest size class), the function bypasses the normal pooled allocation system and directly requests page runs from the free page manager. For smaller allocations, it maps the requested size to an appropriate size class and allocates from the corresponding object pool.

The allocation process involves acquiring appropriate locks, finding or creating segments with sufficient space, and initializing the necessary metadata structures including spans and page maps.

## Parameters / Member Variables
- : Pointer to the DSA area from which to allocate memory
- : Number of bytes to allocate (must be greater than 0)
- : Bitmap controlling allocation behavior, constructed from:
  - : Allows allocations >= 1GB
  - : Returns InvalidDsaPointer on failure instead of raising ERROR
  - : Zero-initializes the allocated memory

## Dependencies
- Functions called/Symbols referenced:
  - , 
  - , 
  - 
  - , 
  - 
  - , 
  - , 
- Called from:
  -  (macro wrapper)
  -  (macro wrapper)
  - 
  -  (in dshash.c)
  - 

## Notes and Other Information
- Returns  on allocation failure when  is set
- Large allocations (> largest size class) use a special span management system
- Small allocations use a size class mapping system with lookup tables and binary search
- Thread-safe through appropriate locking mechanisms
- Memory contents are indeterminate unless  flag is used
- The function enforces size limits and validates allocation requests for safety

## Simplified Source

```c
// Simplified version of dsa_allocate_extended
dsa_pointer
dsa_allocate_extended(dsa_area *area, size_t size, int flags)
{
    uint16 size_class;
    dsa_pointer result;

    Assert(size > 0);

    // Validate allocation size based on flags
    if (((flags & DSA_ALLOC_HUGE) != 0 && !AllocHugeSizeIsValid(size)) ||
        ((flags & DSA_ALLOC_HUGE) == 0 && !AllocSizeIsValid(size)))
        elog(ERROR, "invalid DSA memory alloc request size %zu", size);

    // Handle large allocations (bigger than largest size class)
    if (size > dsa_size_classes[lengthof(dsa_size_classes) - 1])
    {
        size_t npages = fpm_size_to_pages(size);
        dsa_pointer span_pointer;
        dsa_segment_map *segment_map;
        size_t first_page;
        dsa_pointer start_pointer;

        // Get span object for large allocation
        span_pointer = alloc_object(area, DSA_SCLASS_BLOCK_OF_SPANS);
        if (!DsaPointerIsValid(span_pointer))
        {
            if ((flags & DSA_ALLOC_NO_OOM) == 0)
                ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY),
                               errmsg("out of memory")));
            return InvalidDsaPointer;
        }

        // Find or create segment with enough pages
        LWLockAcquire(DSA_AREA_LOCK(area), LW_EXCLUSIVE);
        segment_map = get_best_segment(area, npages);
        if (segment_map == NULL)
            segment_map = make_new_segment(area, npages);

        if (segment_map == NULL)
        {
            // No more segments available
            LWLockRelease(DSA_AREA_LOCK(area));
            dsa_free(area, span_pointer);
            if ((flags & DSA_ALLOC_NO_OOM) == 0)
                ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY),
                               errmsg("out of memory")));
            return InvalidDsaPointer;
        }

        // Allocate pages from free page manager
        if (!FreePageManagerGet(segment_map->fpm, npages, &first_page))
            elog(FATAL, "dsa_allocate could not find %zu free pages", npages);
        LWLockRelease(DSA_AREA_LOCK(area));

        // Set up span and return pointer to allocated pages
        start_pointer = DSA_MAKE_POINTER(get_segment_index(area, segment_map),
                                        first_page * FPM_PAGE_SIZE);
        LWLockAcquire(DSA_SCLASS_LOCK(area, DSA_SCLASS_SPAN_LARGE), LW_EXCLUSIVE);
        init_span(area, span_pointer, &area->control->pools[DSA_SCLASS_SPAN_LARGE],
                 start_pointer, npages, DSA_SCLASS_SPAN_LARGE);
        segment_map->pagemap[first_page] = span_pointer;
        LWLockRelease(DSA_SCLASS_LOCK(area, DSA_SCLASS_SPAN_LARGE));

        // Zero memory if requested
        if ((flags & DSA_ALLOC_ZERO) != 0)
            memset(dsa_get_address(area, start_pointer), 0, size);

        return start_pointer;
    }

    // Handle normal-sized allocations: map size to size class
    if (size < lengthof(dsa_size_class_map) * DSA_SIZE_CLASS_MAP_QUANTUM)
    {
        // Use lookup table for smaller sizes
        int mapidx = ((size + DSA_SIZE_CLASS_MAP_QUANTUM - 1) /
                     DSA_SIZE_CLASS_MAP_QUANTUM) - 1;
        size_class = dsa_size_class_map[mapidx];
    }
    else
    {
        // Binary search for larger sizes
        uint16 min = dsa_size_class_map[lengthof(dsa_size_class_map) - 1];
        uint16 max = lengthof(dsa_size_classes) - 1;

        while (min < max)
        {
            uint16 mid = (min + max) / 2;
            if (dsa_size_classes[mid] < size)
                min = mid + 1;
            else
                max = mid;
        }
        size_class = min;
    }

    // Allocate object from appropriate size class pool
    result = alloc_object(area, size_class);

    // Handle allocation failure
    if (!DsaPointerIsValid(result))
    {
        if ((flags & DSA_ALLOC_NO_OOM) == 0)
            ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY),
                           errmsg("out of memory")));
        return InvalidDsaPointer;
    }

    // Zero memory if requested
    if ((flags & DSA_ALLOC_ZERO) != 0)
        memset(dsa_get_address(area, result), 0, size);

    return result;
}
```

Key simplifications made:
- Consolidated error handling patterns into consistent blocks
- Removed detailed error messages while preserving core error reporting
- Simplified variable declarations and initialization
- Added clear comments explaining the two main allocation paths (large vs normal)
- Maintained essential locking and memory management logic
- Preserved all critical functionality while improving readability
- Reduced from ~172 lines to ~95 lines while keeping essential algorithm intact