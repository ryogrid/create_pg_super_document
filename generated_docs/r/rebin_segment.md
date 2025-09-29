# rebin_segment

## Location
[src/backend/utils/mmgr/dsa.c:2316-2342](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/dsa.c#L2316-L2342)

## Overview
Moves a DSA segment to the appropriate bin based on its current largest contiguous free space, maintaining optimal segment organization for allocation efficiency.

## Definition
```c
static void rebin_segment(dsa_area *area, dsa_segment_map *segment_map)
```

## Detailed Description
This function implements segment reorganization within a Dynamic Shared Area (DSA) by moving segments between bins when their available free space characteristics change. The function determines the appropriate bin for a segment based on its largest contiguous free space block, as tracked by the segment's free page manager (FPM).

When a segment's largest free block size changes due to allocations or deallocations, it may no longer belong in its current bin. This function detects such cases and moves the segment to the correct bin to maintain the DSA's size-based organization, which is crucial for efficient allocation algorithms that search bins in size order.

The rebinning process involves unlinking the segment from its current bin, updating its bin identifier, and linking it to the front of the new bin's doubly-linked list. This maintains the invariant that segments in each bin have free space appropriate for that bin's size range.

## Parameters / Member Variables
- `area`: Pointer to the DSA area containing the segment bin structures
- `segment_map`: Pointer to the segment map structure representing the segment to be rebinned

## Dependencies
- Functions called/Symbols referenced:
  - [contiguous_pages_to_segment_bin](../c/contiguous_pages_to_segment_bin.md) (determines appropriate bin for given page count)
  - fpm_largest (gets largest contiguous free block from free page manager)
  - [unlink_segment](../u/unlink_segment.md) (removes segment from current bin linked list)
  - get_segment_index (converts segment map to index)
  - [get_segment_by_index](../g/get_segment_by_index.md) (converts segment index to map)
- Called from (representative examples):
  - get_segment_index
  - [dsa_free](../d/dsa_free.md)
  - [destroy_superblock](../d/destroy_superblock.md)
  - [get_best_segment](../g/get_best_segment.md)

## Notes and Other Information
- This is a static internal function not exposed in the public API
- Early return optimization when segment is already in the correct bin
- Maintains doubly-linked list integrity by updating both prev and next pointers
- Uses DSA_SEGMENT_INDEX_NONE constant for null segment references
- Critical for maintaining DSA allocation performance by keeping segments properly organized by available space
- The segment is always placed at the front of its new bin for insertion efficiency

## Simplified Source

```c
static void
rebin_segment(dsa_area *area, dsa_segment_map *segment_map)
{
    size_t new_bin;
    dsa_segment_index segment_index;

    // Determine which bin this segment should be in based on largest free space
    new_bin = contiguous_pages_to_segment_bin(fmp_largest(segment_map->fpm));

    // If already in correct bin, nothing to do
    if (segment_map->header->bin == new_bin)
        return;

    // Remove segment from its current bin
    unlink_segment(area, segment_map);

    // Add segment to front of new bin's linked list
    segment_index = get_segment_index(area, segment_map);
    segment_map->header->prev = DSA_SEGMENT_INDEX_NONE;
    segment_map->header->next = area->control->segment_bins[new_bin];
    segment_map->header->bin = new_bin;
    area->control->segment_bins[new_bin] = segment_index;

    // Update next segment's prev pointer if it exists
    if (segment_map->header->next != DSA_SEGMENT_INDEX_NONE) {
        dsa_segment_map *next;
        next = get_segment_by_index(area, segment_map->header->next);
        next->header->prev = segment_index;
    }
}
```