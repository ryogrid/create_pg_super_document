# _bt_strategy

## Location
[src/backend/access/nbtree/nbtsplitloc.c:934-1051](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtsplitloc.c#L934-L1051)

## Overview
Determines the optimal B-tree splitting strategy and calculates the perfect penalty score based on page characteristics, particularly for handling duplicate values and tuple size optimization.

## Definition

```c
static int
_bt_strategy(FindSplitData *state, SplitPoint *leftpage,
			 SplitPoint *rightpage, FindSplitStrat *strategy)
```
## Detailed Description
This function analyzes the page being split and decides between three splitting strategies: default, many duplicates, or single value. It examines the distribution of key values and determines which strategy will produce the most efficient split while minimizing tuple size overhead. For internal pages, it returns the minimum first-right tuple size as the perfect penalty. For leaf pages, it performs more complex analysis of duplicate patterns and key distribution.

The function returns a "perfect penalty" value that represents the theoretical best case for avoiding heap TID appendages in high keys. This value is used by _bt_bestsplitloc() to optimize split point selection and potentially terminate early when the perfect score is achieved.

## Parameters / Member Variables
- : FindSplitData structure containing page and split candidate information
- : SplitPoint representing the leftmost possible split point
- : SplitPoint representing the rightmost possible split point  
- : Output parameter for the recommended splitting strategy (SPLIT_DEFAULT, SPLIT_MANY_DUPLICATES, or SPLIT_SINGLE_VALUE)

## Dependencies
- Functions called/Symbols referenced:
  - IndexRelationGetNumberOfKeyAttributes
  - [_bt_interval_edges](_bt_interval_edges.md)
  - [_bt_split_lastleft](_bt_split_lastleft.md)
  - [_bt_split_firstright](_bt_split_firstright.md)
  - [_bt_keep_natts_fast](_bt_keep_natts_fast.md)
  - [PageGetItemId](../P/PageGetItemId.md)
  - [PageGetItem](../P/PageGetItem.md)
- Structures/Types/Constants referenced:
  - FindSplitData
  - SplitPoint
  - FindSplitStrat
  - SPLIT_DEFAULT, SPLIT_MANY_DUPLICATES, SPLIT_SINGLE_VALUE
  - [IndexTuple](../I/IndexTuple.md), ItemId
  - P_HIKEY
- Called from (representative examples):
  - [_bt_findsplitloc](_bt_findsplitloc.md)

## Notes and Other Information
- This is a static function used only within nbtsplitloc.c for B-tree split strategy selection
- For internal pages, simply returns minfirstrightsz to optimize common cases with uniform tuple sizes
- For leaf pages, performs sophisticated analysis of duplicate value patterns
- The many duplicates strategy helps handle pages with large groups of identical values
- The single value strategy is used for rightmost pages with ever-increasing heap TIDs
- Returns indnkeyatts instead of true perfect penalty for many duplicates to prevent unbalanced splits in low cardinality composite indexes
- Strategy selection affects both split point choice and subsequent split interval calculation