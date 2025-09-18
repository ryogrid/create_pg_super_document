# _bt_findsplitloc

## Location
src/backend/access/nbtree/nbtsplitloc.c: 129 - 448

## Overview
Finds an appropriate split point for a B-tree page, balancing space utilization while considering the new item to be inserted and optimizing for suffix truncation effectiveness.

## Definition
```c
OffsetNumber _bt_findsplitloc(Relation rel, Page origpage, OffsetNumber newitemoff, Size newitemsz, IndexTuple newitem, bool *newitemonleft)
```

## Detailed Description
This function determines the optimal location to split a B-tree page when inserting a new item. The primary goal is to equalize free space on both sides of the split after accounting for the new item. For rightmost pages, it applies a fill factor strategy to maintain consistent page density during sequential insertions.

The function implements multiple split strategies:
1. **Default strategy**: Balances space while considering suffix truncation effectiveness on leaf pages
2. **Many duplicates strategy**: Widens the split interval when dealing with many duplicate values
3. **Single value strategy**: Used when all values are identical, favoring high fill factor on the left page

The algorithm evaluates all possible split points, calculates space utilization for each, and selects the optimal point based on the chosen strategy. For leaf pages, it considers suffix truncation benefits by preferring splits that allow more trailing attributes to be truncated from the high key.

## Parameters / Member Variables
- `rel`: B-tree relation being split
- `origpage`: Original page that needs to be split
- `newitemoff`: Offset number where the new item should be inserted
- `newitemsz`: Size of the new item (MAXALIGNED, excluding line pointer)
- `newitem`: The new index tuple to be inserted
- `newitemonleft`: Output parameter indicating whether new item goes on left or right page

## Dependencies
- Functions called/Symbols referenced:
  - BTPageGetOpaque: Get page opaque data
  - PageGetMaxOffsetNumber: Get maximum offset number
  - PageGetExactFreeSpace: Calculate available free space
  - BTGetFillFactor: Get relation fill factor
  - _bt_recsplitloc: Record potential split locations
  - _bt_afternewitemoff: Check for split-after-new-item optimization
  - _bt_deltasortsplits: Sort split points by delta values
  - _bt_defaultinterval: Calculate default split interval
  - _bt_strategy: Determine split strategy
  - _bt_bestsplitloc: Select best split point from candidates
- Called from:
  - _bt_split: Main page splitting function

## Notes and Other Information
- Returns the offset number of the first tuple that should go on the right page
- The function never fails to find a feasible split point, but includes error handling for safety
- Special handling for rightmost pages to maintain consistent fill factors during sequential insertions
- Considers posting list items and ensures newitem cannot be a posting list item
- The split location affects both space utilization and suffix truncation effectiveness on leaf pages
- Uses different fill factor strategies for leaf vs non-leaf pages