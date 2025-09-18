# _bt_defaultinterval

## Location
src/backend/access/nbtree/nbtsplitloc.c: 876 - 933

## Overview
Determines the split interval for the default B-tree splitting strategy, which limits the number of candidate split points that receive further consideration based on balanced free space distribution.

## Definition


## Detailed Description
This function calculates an acceptable range of split points (split interval) that have reasonably balanced leftfree and rightfree values. The split interval represents the number of candidate split points from the sorted splits array that should be considered for the final split decision. The function implements tolerance-based filtering where split points that divide free space too unevenly are excluded from consideration.

The function follows the "Prefix B-Trees" paper methodology, where the split interval is called sigma l for leaf splits and sigma b for internal splits. The resulting split interval is typically about 10% of total splits for uniformly-sized tuples on leaf pages, with more aggressive filtering for internal pages.

## Parameters / Member Variables
- : FindSplitData structure containing split candidates, page information, and whether this is a leaf page split

## Dependencies
- Functions called/Symbols referenced:
  - LEAF_SPLIT_DISTANCE (constant)
  - INTERNAL_SPLIT_DISTANCE (constant)
- Structures/Types referenced:
  - FindSplitData
  - SplitPoint
- Called from (representative examples):
  - _bt_findsplitloc

## Notes and Other Information
- This is a static function used only within nbtsplitloc.c for default B-tree split strategy
- Uses different tolerance levels for leaf vs internal page splits (leaf pages are less aggressive)
- The tolerance is calculated as a percentage of olddataitemstotal to account for varying tuple sizes
- Returns the number of split points that fall within acceptable balance tolerances
- The split interval concept helps reduce tuple sizes on higher index levels without significantly affecting space utilization
- Implementation may need adjustment if suffix truncation is extended to truncate within individual attributes/datums