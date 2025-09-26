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
- : Pointer to the dynamic shared area containing the segment management structures
- : Number of contiguous pages required for the allocation request

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