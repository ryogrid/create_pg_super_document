# unlink_segment

## Location
src/backend/utils/mmgr/dsa.c: 1978 - 2009

## Overview
Removes a segment from the linked list bin that contains it by updating the previous and next pointers in the doubly-linked list structure.

## Definition


## Detailed Description
The  function removes a segment from a bin (doubly-linked list) within the dynamic shared area's memory management system. It handles three pointer update scenarios:

1. **Previous segment exists**: Updates the previous segment's next pointer to skip over the current segment
2. **No previous segment (head of list)**: Updates the bin head pointer in the area control structure to point to the next segment
3. **Next segment exists**: Updates the next segment's previous pointer to point to the current segment's previous

The function maintains the integrity of the doubly-linked list by ensuring all pointers are properly updated when a segment is removed from its bin.

## Parameters / Member Variables
- : Pointer to the dynamic shared area containing the segment management structures
- : Pointer to the segment map structure representing the segment to be unlinked from its bin

## Dependencies
- Functions called/Symbols referenced:
  - get_segment_by_index
  - get_segment_index
  - DSA_SEGMENT_INDEX_NONE (constant)
- Called from (representative examples):
  - destroy_superblock
  - rebin_segment

## Notes and Other Information
- This is a static (internal) function used for segment bin management
- The function uses  to check for null pointers in the linked list
- Includes assertion checking to verify the bin head pointer consistency when unlinking the first element
- The unlinking operation is atomic in terms of maintaining list consistency
- Used in memory management operations when segments need to be moved between bins or removed entirely
- Part of the dynamic shared area's internal bookkeeping for free space management