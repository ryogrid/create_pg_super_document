# bms_num_members

## Location
src/backend/nodes/bitmapset.c: 751 - 780

## Overview
Counts and returns the total number of members (set bits) in a bitmapset.

## Definition
```c
int bms_num_members(const Bitmapset *a)
```

## Detailed Description
This function efficiently counts the number of set bits across all words in a bitmapset. It iterates through each word in the bitmapset and uses the bmw_popcount() function to count the number of set bits in each non-zero word. The function optimizes performance by skipping zero words entirely, as they contribute no set bits to the total count. The population count (popcount) operation is typically implemented using efficient bit manipulation techniques or specialized CPU instructions.

## Parameters / Member Variables
- `a`: The bitmapset whose members are to be counted (const Bitmapset *)

## Dependencies
- Functions called/Symbols referenced:
  - [bms_is_valid_set](bms_is_valid_set.md)
  - bitmapword
  - bmw_popcount
- Called from (representative examples):
  - [ExecCheckPermissions](../E/ExecCheckPermissions.md) (src/backend/executor/execMain.c:606)
  - [find_hash_columns](../f/find_hash_columns.md) (src/backend/executor/nodeAgg.c:1628)
  - [ExecInitAppend](../E/ExecInitAppend.md) (src/backend/executor/nodeAppend.c:151)
  - [get_memoize_path](../g/get_memoize_path.md) (src/backend/optimizer/path/joinpath.c:655)
  - [adjust_group_pathkeys_for_groupagg](../a/adjust_group_pathkeys_for_groupagg.md) (src/backend/optimizer/plan/planner.c:3382, 3468)
  - build_join_rel (src/backend/optimizer/util/relnode.c:859)
  - [make_partition_pruneinfo](../m/make_partition_pruneinfo.md) (src/backend/partitioning/partprune.c:346)
  - [BuildRelationExtStatistics](../B/BuildRelationExtStatistics.md) (src/backend/statistics/extended_stats.c:186)

## Notes and Other Information
- Returns 0 for NULL (empty) bitmapsets
- Uses efficient bmw_popcount() for bit counting in each word
- Skips zero words as an optimization since they contain no set bits
- Widely used throughout PostgreSQL for cardinality estimation and resource planning
- Essential for query optimization decisions based on set sizes
- Located in src/backend/nodes/bitmapset.c:751-780