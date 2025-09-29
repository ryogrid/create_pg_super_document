# destroy_superblock

## Location
[src/backend/utils/mmgr/dsa.c:1837-1905](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/dsa.c#L1837-L1905)

## Overview
Returns a superblock to the free page manager and potentially frees the underlying segment if it becomes entirely free.

## Definition

```c
static void
destroy_superblock(dsa_area *area, dsa_pointer span_pointer)
```
## Detailed Description
This function handles the destruction of a superblock when it becomes completely empty (all objects have been freed). It performs several critical operations: removes the span from its fullness class list, returns the pages to the segment's free page manager, checks if the entire segment has become free, and if so, returns the segment to the operating system.

The function implements a careful locking protocol where it acquires the area lock while already holding a per-pool lock. This specific order (pool lock first, then area lock) prevents deadlocks that could occur if the locks were acquired in the opposite order.

When an entire segment becomes free (indicated by the free page manager's largest available space equaling the segment's total usable pages), the segment is returned to the OS unless it's the special segment 0 that contains extra control data. The segment cleanup process includes updating the total segment size, unpinning and detaching the DSM segment, invalidating the segment handle, and clearing the local segment map.

For blocks other than span-of-spans blocks, the function recursively calls dsa_free() to free the span descriptor object itself, since these descriptors are allocated separately rather than being stored inline within the block.

## Parameters / Member Variables
- : The DSA area containing the segment to potentially destroy
- : Pointer to the span describing the superblock to be destroyed

## Dependencies
- Functions called/Symbols referenced:
  - [unlink_span](../u/unlink_span.md)
  - [get_segment_by_index](../g/get_segment_by_index.md)
  - [FreePageManagerPut](../F/FreePageManagerPut.md)
  - fpm_largest
  - get_segment_index
  - [unlink_segment](../u/unlink_segment.md)
  - [dsm_unpin_segment](dsm_unpin_segment.md)
  - [dsm_detach](dsm_detach.md)
  - [rebin_segment](../r/rebin_segment.md)
  - [dsa_free](dsa_free.md)
- Called from (representative examples):
  - [dsa_free](dsa_free.md)
  - [dsa_trim](dsa_trim.md)

## Notes and Other Information
The function must be called with the appropriate pool lock held. It implements a specific lock ordering discipline to avoid deadlocks and uses the freed_segment_counter to help other backends detect when segments have been freed. Special handling is required for DSA_SCLASS_BLOCK_OF_SPANS where the span descriptor is stored inline.

## Simplified Source

```c
static void
destroy_superblock(dsa_area *area, dsa_pointer span_pointer)
{
    dsa_area_span *span = dsa_get_address(area, span_pointer);
    int size_class = span->size_class;
    dsa_segment_map *segment_map;

    // Remove span from its fullness class list
    unlink_span(area, span);

    // Acquire area lock (note: pool lock already held)
    LWLockAcquire(DSA_AREA_LOCK(area), LW_EXCLUSIVE);
    check_for_freed_segments_locked(area);

    // Get segment and return pages to free page manager
    segment_map = get_segment_by_index(area, DSA_EXTRACT_SEGMENT_NUMBER(span->start));
    FreePageManagerPut(segment_map->fpm,
                       DSA_EXTRACT_OFFSET(span->start) / FPM_PAGE_SIZE,
                       span->npages);

    // Check if entire segment is now free
    if (fpm_largest(segment_map->fpm) == segment_map->header->usable_pages) {
        dsa_segment_index index = get_segment_index(area, segment_map);

        // Free segment if it's not the special segment 0
        if (index != 0) {
            // Return segment to OS and update tracking
            unlink_segment(area, segment_map);
            segment_map->header->freed = true;
            area->control->total_segment_size -= segment_map->header->size;
            dsm_unpin_segment(dsm_segment_handle(segment_map->segment));
            dsm_detach(segment_map->segment);
            area->control->segment_handles[index] = DSM_HANDLE_INVALID;
            ++area->control->freed_segment_counter;

            // Clear segment mapping
            segment_map->segment = NULL;
            segment_map->header = NULL;
            segment_map->mapped_address = NULL;
        }
    }

    // Move segment to appropriate bin if still exists
    if (segment_map->header != NULL)
        rebin_segment(area, segment_map);

    LWLockRelease(DSA_AREA_LOCK(area));

    // Free span descriptor if not span-of-spans block
    if (size_class != DSA_SCLASS_BLOCK_OF_SPANS)
        dsa_free(area, span_pointer);
}
```
