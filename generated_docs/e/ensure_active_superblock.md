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
