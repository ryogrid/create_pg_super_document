# _bt_recsplitloc

## Location
src/backend/access/nbtree/nbtsplitloc.c: 449 - 565

## Overview
Records a potential split point between two tuples on a B-tree page, calculating space utilization and verifying the split legality before storing it for later analysis.

## Definition
```c
static void _bt_recsplitloc(FindSplitData *state, OffsetNumber firstrightoff, bool newitemonleft, int olddataitemstoleft, Size firstrightofforigpagetuplesz)
```

## Detailed Description
This function evaluates a specific split location and records it if the split would be legal (both sides have non-negative free space). It performs detailed space calculations accounting for:

1. **High key overhead**: The first item on the right page becomes the high key for the left page, consuming space on both sides
2. **Suffix truncation benefits**: On leaf pages, estimates space savings from truncating posting lists in the high key
3. **Key data discarding**: On non-leaf pages, accounts for discarding key data from the first right page item
4. **New item placement**: Calculates space impact based on whether the new item goes left or right

The function implements conservative space estimation, particularly for leaf pages where it assumes suffix truncation cannot avoid adding a heap TID to the high key. It also optimizes for posting list tuples by subtracting posting list overhead when it would make an appreciable difference.

## Parameters / Member Variables
- `state`: FindSplitData structure containing split state and accumulated results
- `firstrightoff`: Offset number of the first item that goes to the right page
- `newitemonleft`: Whether the new item should be placed on the left page
- `olddataitemstoleft`: Total size of old data items to the left of the split point
- `firstrightofforigpagetuplesz`: Size of the tuple at firstrightoff position

## Dependencies
- Functions called/Symbols referenced:
  - PageGetItemId: Get item ID from page
  - PageGetItem: Get item data from page
  - BTreeTupleIsPosting: Check if tuple is a posting list tuple
  - IndexTupleSize: Get total size of index tuple
  - BTreeTupleGetPostingOffset: Get posting list offset within tuple
- Called from:
  - _bt_findsplitloc: Main split location finder (multiple call sites)

## Notes and Other Information
- Only records splits where both left and right sides have non-negative free space
- Tracks the minimum first-right tuple size among all legal splits
- Handles special case where the new item becomes the first-right tuple
- For posting list tuples over 64 bytes, calculates potential space savings from suffix truncation
- Assumes worst-case scenario for high key size on leaf pages to ensure safe split decisions
- The split point represents a position between two adjacent tuples on the imaginary combined page