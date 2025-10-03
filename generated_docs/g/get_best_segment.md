# get_best_segment

## Location
[src/backend/utils/mmgr/dsa.c:2010-2080](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/dsa.c#L2010-L2080)

## Overview
Searches through segment bins to find a segment that can satisfy a memory allocation request for a specified number of contiguous pages.

## Definition

```c
static dsa_segment_map *
get_best_segment(dsa_area *area, size_t npages)
```
## Detailed Description
The  function implements a bin-based search strategy to locate a segment with sufficient contiguous free pages. It starts searching from the smallest bin that might contain segments with the required number of contiguous pages and iterates through larger bins if necessary.

The function performs several optimizations:
1. **Bin-based search**: Uses  to determine the optimal starting bin
2. **Dynamic re-binning**: Moves segments to appropriate bins if their free space no longer matches their current bin
3. **Early termination**: Returns immediately when a suitable segment is found
4. **Freed segment cleanup**: Calls  before searching

The search algorithm examines each segment in a bin, checks its largest contiguous free space using , and either returns the segment if it's suitable or continues to the next segment/bin.

## Parameters / Member Variables
- `*area`: Pointer to the dynamic shared area containing the segment management structures
- `npages`: Number of contiguous pages required for the allocation request
## Dependencies
- Functions called/Symbols referenced:
  - [LWLockHeldByMe](../L/LWLockHeldByMe.md) / DSA_AREA_LOCK (lock assertion)
  - [check_for_freed_segments_locked](../c/check_for_freed_segments_locked.md)
  - [contiguous_pages_to_segment_bin](../c/contiguous_pages_to_segment_bin.md)
  - [get_segment_by_index](get_segment_by_index.md)
  - fpm_largest
  - [rebin_segment](../r/rebin_segment.md)
  - DSA_NUM_SEGMENT_BINS (constant)
  - DSA_SEGMENT_INDEX_NONE (constant)
- Called from (representative examples):
  - [dsa_allocate_extended](../d/dsa_allocate_extended.md)
  - [ensure_active_superblock](../e/ensure_active_superblock.md)

## Notes and Other Information
- This is a static (internal) function used for memory allocation within DSA
- Must be called with the DSA area lock held (enforced by assertion)
- Uses a threshold-based approach where each bin represents segments with at least 2^(bin-1) contiguous pages
- Implements intelligent re-binning to maintain bin integrity over time
- Returns NULL if no suitable segment is found, indicating that a new segment may need to be created
- The search strategy balances efficiency with accuracy by starting from the most promising bins
- Part of the dynamic shared area's sophisticated memory management system

## Simplified Source

```c
static dsa_segment_map *get_best_segment(dsa_area *area, size_t npages)
{
    size_t bin;

    Assert(LWLockHeldByMe(DSA_AREA_LOCK(area)));
    check_for_freed_segments_locked(area);

    // Start from the bin that might have enough contiguous pages
    for (bin = contiguous_pages_to_segment_bin(npages);
         bin < DSA_NUM_SEGMENT_BINS;
         ++bin) {

        // Minimum contiguous pages for this bin
        size_t threshold = (size_t) 1 << (bin - 1);
        dsa_segment_index segment_index;

        // Search this bin for a suitable segment
        segment_index = area->control->segment_bins[bin];
        while (segment_index != DSA_SEGMENT_INDEX_NONE) {
            dsa_segment_map *segment_map;
            dsa_segment_index next_segment_index;
            size_t contiguous_pages;

            segment_map = get_segment_by_index(area, segment_index);
            next_segment_index = segment_map->header->next;
            contiguous_pages = fpm_largest(segment_map->fpm);

            // Skip if not enough for request but still valid for this bin
            if (contiguous_pages >= threshold && contiguous_pages < npages) {
                segment_index = next_segment_index;
                continue;
            }

            // Re-bin segment if it no longer fits this bin
            if (contiguous_pages < threshold) {
                rebin_segment(area, segment_map);
                // Continue to check if it still satisfies our request
            }

            // Found a suitable segment
            if (contiguous_pages >= npages)
                return segment_map;

            segment_index = next_segment_index;
        }
    }

    return NULL;  // No suitable segment found
}
```