# _bt_split_penalty

## Location
src/backend/access/nbtree/nbtsplitloc.c: 1131 - 1158

## Overview
Calculates a penalty score for a B-tree split point candidate, helping to determine the optimal location to split a B-tree page during node splits.

## Definition


## Detailed Description
This function computes a penalty score for a given split point to help the B-tree split algorithm choose the best location to split a page. The penalty calculation differs significantly between leaf and internal (non-leaf) pages:

**For internal pages**: The penalty is simply the size of the first tuple that would go to the right page (including line pointer overhead). This tuple will become the new high key for the left page.

**For leaf pages**: The penalty represents the attribute number that distinguishes each side of the split. It indicates the last attribute that needs to be included in the new high key for the left page. This value can exceed the number of key attributes when a heap TID needs to be appended during key truncation.

The function is designed as a subroutine to support the overall B-tree page splitting strategy by providing a quantitative measure to compare different split point candidates.

## Parameters / Member Variables
- : Pointer to FindSplitData structure containing context information for the split operation, including the relation, original page, and new item details
- : Pointer to SplitPoint structure representing the candidate split point being evaluated

## Dependencies
- Functions called/Symbols referenced:
  - PageGetItemId
  - ItemIdGetLength
  - _bt_split_lastleft
  - _bt_split_firstright
  - _bt_keep_natts_fast
- Called from (representative examples):
  - _bt_bestsplitloc
  - FindSplitData (structure usage)

## Notes and Other Information
- This is a static inline function optimized for performance as it's called frequently during B-tree operations
- The penalty scoring system is crucial for B-tree performance as it affects page utilization and future split patterns
- For internal pages, the penalty directly relates to storage overhead, while for leaf pages it relates to key truncation efficiency
- The function handles special cases where the new item being inserted affects the split point calculation