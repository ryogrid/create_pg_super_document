# unlink_segment

## Location
[src/backend/utils/mmgr/dsa.c:1978-2009](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/dsa.c#L1978-L2009)

## Overview
Removes a segment from the linked list bin that contains it by updating the previous and next pointers in the doubly-linked list structure.

## Definition

```c
static void
unlink_segment(dsa_area *area, dsa_segment_map *segment_map)
```
## Detailed Description
The  function removes a segment from a bin (doubly-linked list) within the dynamic shared area's memory management system. It handles three pointer update scenarios:

1. **Previous segment exists**: Updates the previous segment's next pointer to skip over the current segment
2. **No previous segment (head of list)**: Updates the bin head pointer in the area control structure to point to the next segment
3. **Next segment exists**: Updates the next segment's previous pointer to point to the current segment's previous

The function maintains the integrity of the doubly-linked list by ensuring all pointers are properly updated when a segment is removed from its bin.

## Parameters / Member Variables
- `*area`: Pointer to the dynamic shared area containing the segment management structures
- `*segment_map`: Pointer to the segment map structure representing the segment to be unlinked from its bin
## Dependencies
- Functions called/Symbols referenced:
  - [get_segment_by_index](../g/get_segment_by_index.md)
  - get_segment_index
  - DSA_SEGMENT_INDEX_NONE (constant)
- Called from (representative examples):
  - [destroy_superblock](../d/destroy_superblock.md)
  - [rebin_segment](../r/rebin_segment.md)

## Notes and Other Information
- This is a static (internal) function used for segment bin management
- The function uses  to check for null pointers in the linked list
- Includes assertion checking to verify the bin head pointer consistency when unlinking the first element
- The unlinking operation is atomic in terms of maintaining list consistency
- Used in memory management operations when segments need to be moved between bins or removed entirely
- Part of the dynamic shared area's internal bookkeeping for free space management

## Simplified Source

```c
static void
unlink_segment(dsa_area *area, dsa_segment_map *segment_map)
{
    // Update previous segment's next pointer (or bin head if first)
    if (segment_map->header->prev != DSA_SEGMENT_INDEX_NONE)
    {
        dsa_segment_map *prev = get_segment_by_index(area, segment_map->header->prev);
        prev->header->next = segment_map->header->next;
    }
    else
    {
        // This was the head of the bin, update bin pointer
        Assert(area->control->segment_bins[segment_map->header->bin] ==
               get_segment_index(area, segment_map));
        area->control->segment_bins[segment_map->header->bin] =
            segment_map->header->next;
    }

    // Update next segment's previous pointer
    if (segment_map->header->next != DSA_SEGMENT_INDEX_NONE)
    {
        dsa_segment_map *next = get_segment_by_index(area, segment_map->header->next);
        next->header->prev = segment_map->header->prev;
    }
}
```

**Simplified Explanation:**
1. If segment has a previous segment, update its next pointer to skip current segment
2. If segment is head of bin (no previous), update the bin head pointer to next segment
3. If segment has a next segment, update its previous pointer to skip current segment
4. This maintains doubly-linked list integrity when removing a segment from its bin