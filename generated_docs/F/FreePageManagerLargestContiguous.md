# FreePageManagerLargestContiguous

## Location
src/backend/utils/mmgr/freepage.c: 324 - 365

## Overview
Computes and returns the size of the largest contiguous run of pages that could be successfully allocated from the free page manager.

## Definition
```c
static Size FreePageManagerLargestContiguous(FreePageManager *fpm)
```

## Detailed Description
FreePageManagerLargestContiguous determines the maximum number of contiguous pages available for allocation by examining the free page manager's data structures. The function employs an optimized search strategy that takes advantage of the freelist organization.

The algorithm works in two phases:
1. **Large span check**: First examines the highest freelist (FPM_NUM_FREELISTS - 1) which contains spans too large for smaller freelists. If this list is non-empty, it traverses all spans to find the maximum size.
2. **Fallback search**: If the largest freelist is empty, it searches downward through smaller freelists to find the highest non-empty list, whose index + 1 represents the largest available span size.

This approach is efficient because the freelist organization naturally groups spans by size ranges, allowing the function to quickly identify the largest available contiguous range without exhaustive searching.

## Parameters / Member Variables
- `fpm`: Pointer to the FreePageManager structure to analyze

## Dependencies
- Functions called/Symbols referenced:
  - fpm_segment_base (gets base address for relative pointer operations)
  - relptr_access (accesses span nodes via relative pointers)
  - relptr_is_null (checks for null relative pointers)
- Types/Constants referenced:
  - FreePageManager
  - FreePageSpanLeader
  - FPM_NUM_FREELISTS
- Called from:
  - FreePageManagerGet (debug assertion)
  - FreePageManagerUpdateLargest (cached value computation)
  - FreePageManagerPut (debug assertion)

## Notes and Other Information
- This is a static function used internally for tracking largest contiguous ranges
- The function takes advantage of freelist organization for efficient searching
- Used both for cached value computation and debug verification
- The two-phase algorithm provides optimal performance for different data distribution patterns
- Results are used to maintain the contiguous_pages field in the FreePageManager
- Essential for allocation decisions and space management optimization