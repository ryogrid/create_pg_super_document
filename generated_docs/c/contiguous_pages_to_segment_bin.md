# contiguous_pages_to_segment_bin

## Location
[src/backend/utils/mmgr/dsa.c:119-131](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/dsa.c#L119-L131)

## Overview
Calculates the lowest segment bin that might contain segments with n contiguous free pages, used for optimizing memory allocation in PostgreSQL's Dynamic Shared Area (DSA) system.

## Definition

```c
static inline size_t
contiguous_pages_to_segment_bin(size_t n)
```
## Detailed Description
This inline static function is a key optimization component in PostgreSQL's DSA memory management system. It determines which segment bin should be searched first when looking for n contiguous free pages. The function implements a logarithmic binning strategy where segments are organized into bins based on their largest contiguous free space. By returning the lowest bin that *might* contain the required pages, it allows the allocation algorithm to skip bins that definitely cannot satisfy the request, significantly improving allocation performance.

The function uses bit manipulation to efficiently calculate the appropriate bin number. For n=0, it returns bin 0. For other values, it uses the position of the leftmost set bit plus one to determine the bin, ensuring that larger requests are directed to higher-numbered bins that are more likely to contain larger free regions.

## Parameters / Member Variables
- : The number of contiguous pages requested for allocation

## Dependencies
- Functions called/Symbols referenced:
  - pg_leftmost_one_pos_size_t
  - DSA_NUM_SEGMENT_BINS
- Called from (representative examples):
  - [create_internal](create_internal.md)
  - [get_best_segment](../g/get_best_segment.md)
  - [make_new_segment](../m/make_new_segment.md)
  - [rebin_segment](../r/rebin_segment.md)

## Notes and Other Information
- This is a static inline function for performance optimization in hot allocation paths
- The function implements a power-of-2 based binning strategy for efficient memory management
- The result is capped at DSA_NUM_SEGMENT_BINS - 1 to ensure valid bin indices
- Essential for the DSA's segment organization and allocation efficiency
- Located in src/backend/utils/mmgr/dsa.c:119-131