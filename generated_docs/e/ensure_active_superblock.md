# ensure_active_superblock

## Location
[src/backend/utils/mmgr/dsa.c:1560-1756](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/dsa.c#L1560-L1756)

## Overview
Ensures that fullness class 1 contains an active superblock suitable for allocations, moving spans between fullness classes or allocating new superblocks as necessary.

## Definition

```c
struct a new span to manage it.
	 *
	 * First, get a dsa_area_span object to describe the new superblock block
	 * ... unless this allocation is for a dsa_area_span object, in which case
	 * that's surely not going to work.  We handle that case by storing the
	 * span describing a block-of-spans inline.
	 */
	if (size_class != DSA_SCLASS_BLOCK_OF_SPANS)
	{
		span_pointer = alloc_object(area, DSA_SCLASS_BLOCK_OF_SPANS);
		if (!DsaPointerIsValid(span_pointer))
			return false;
		npages = DSA_PAGES_PER_SUPERBLOCK;
	}

	/* Find or create a segment and allocate the superblock. */
	LWLockAcquire(DSA_AREA_LOCK(area), LW_EXCLUSIVE);
```
## Detailed Description
This function implements the core logic for maintaining an active allocation target in PostgreSQL's Dynamic Shared Area (DSA) memory management system. It ensures that fullness class 1 always contains a superblock that can accommodate new allocations for the specified size class.

The function operates on a fullness class system where blocks are categorized by their utilization percentage (0 to N fullness classes). The active block is maintained in fullness class 1 rather than class 0, following the principle that allocating from moderately full blocks is preferable to using nearly empty blocks, as this helps prevent fragmentation.

The algorithm first searches higher-numbered fullness classes (2 to N-2) for spans that have become less full due to deallocations and should be moved to class 1. If no suitable existing span is found, it attempts to transfer spans from other classes. As a last resort, it allocates a new superblock by obtaining pages from a segment and initializing the necessary span management structures.

## Parameters / Member Variables
- `DSA_SCLASS_BLOCK_OF_SPANS)`: The DSA area containing the memory pools and segments
- `false`: The specific memory pool for the given size class
- `DSA_PAGES_PER_SUPERBLOCK`: The object size class for which an active superblock is needed
## Dependencies
- Functions called/Symbols referenced:
  - [dsa_get_address](../d/dsa_get_address.md)
  - DsaPointerIsValid
  - [transfer_first_span](../t/transfer_first_span.md)
  - [alloc_object](../a/alloc_object.md)
  - [get_best_segment](../g/get_best_segment.md)
  - [make_new_segment](../m/make_new_segment.md)
  - [FreePageManagerGet](../F/FreePageManagerGet.md)
  - [init_span](../i/init_span.md)
  - get_segment_index
- Called from (representative examples):
  - [alloc_object](../a/alloc_object.md)

## Notes and Other Information
The function must be called while holding the size class lock (DSA_SCLASS_LOCK). It handles the special case of DSA_SCLASS_BLOCK_OF_SPANS differently, where the span descriptor is stored inline within the allocated block rather than as a separate allocation. Returns false if no superblock can be made available due to memory constraints.

## Simplified Source

```c
static bool
ensure_active_superblock(dsa_area *area, dsa_area_pool *pool, int size_class)
{
    dsa_pointer span_pointer;
    dsa_pointer start_pointer;
    size_t obsize = dsa_size_classes[size_class];
    size_t nmax;
    int fclass;

    // Calculate max objects per block
    if (size_class == DSA_SCLASS_BLOCK_OF_SPANS)
        nmax = FPM_PAGE_SIZE / obsize - 1;
    else
        nmax = DSA_SUPERBLOCK_SIZE / obsize;

    // 1. Try to find existing spans that should be moved to class 1
    for (fclass = 2; fclass < DSA_FULLNESS_CLASSES - 1; ++fclass)
    {
        span_pointer = pool->spans[fclass];
        while (DsaPointerIsValid(span_pointer))
        {
            dsa_area_span *span = dsa_get_address(area, span_pointer);
            dsa_pointer next_span_pointer = span->nextspan;

            // Calculate correct fullness class for this span
            int tfclass = (nmax - span->nallocatable) * (DSA_FULLNESS_CLASSES - 1) / nmax;

            // Move span to correct class if needed
            if (tfclass < fclass)
            {
                // Remove from current list and add to target list
                // [list manipulation code omitted for brevity]
            }

            span_pointer = next_span_pointer;
        }

        // If we found a suitable block, we're done
        if (DsaPointerIsValid(pool->spans[1]))
            return true;
    }

    // 2. Transfer any available span to class 1 as fallback
    for (fclass = 2; fclass < DSA_FULLNESS_CLASSES - 1; ++fclass)
        if (transfer_first_span(area, pool, fclass, 1))
            return true;

    if (transfer_first_span(area, pool, 0, 1))
        return true;

    // 3. Allocate new superblock as last resort
    size_t npages = 1;
    if (size_class != DSA_SCLASS_BLOCK_OF_SPANS)
    {
        span_pointer = alloc_object(area, DSA_SCLASS_BLOCK_OF_SPANS);
        if (!DsaPointerIsValid(span_pointer))
            return false;
        npages = DSA_PAGES_PER_SUPERBLOCK;
    }

    // Find segment and allocate pages
    LWLockAcquire(DSA_AREA_LOCK(area), LW_EXCLUSIVE);
    segment_map = get_best_segment(area, npages);
    if (segment_map == NULL)
        segment_map = make_new_segment(area, npages);

    if (!FreePageManagerGet(segment_map->fpm, npages, &first_page))
        elog(FATAL, "could not find free pages for superblock");
    LWLockRelease(DSA_AREA_LOCK(area));

    // Initialize the new span
    start_pointer = DSA_MAKE_POINTER(get_segment_index(area, segment_map),
                                     first_page * FPM_PAGE_SIZE);

    if (size_class == DSA_SCLASS_BLOCK_OF_SPANS)
        span_pointer = start_pointer;

    init_span(area, span_pointer, pool, start_pointer, npages, size_class);

    return true;
}
```

**Simplified Explanation:**
1. **Find existing spans**: Look through higher fullness classes for spans that have become less full and should be moved to class 1
2. **Transfer any span**: If no perfect match, transfer any available span from other classes to class 1
3. **Allocate new superblock**: As last resort, create a new superblock by allocating pages and initializing span management
4. **Handle special case**: For block-of-spans, store the span descriptor inline rather than as separate allocation
