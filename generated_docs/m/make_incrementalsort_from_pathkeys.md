# make_incrementalsort_from_pathkeys

## Location
[src/backend/optimizer/plan/createplan.c:6382-6415](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/createplan.c#L6382-L6415)

## Overview
The make_incrementalsort_from_pathkeys function creates an IncrementalSort plan node from pathkey specifications, optimizing for cases where the input data is already partially sorted.

## Definition
```c
static IncrementalSort *
make_incrementalsort_from_pathkeys(Plan *lefttree, List *pathkeys,
                                   Relids relids, int nPresortedCols)
```

## Detailed Description
The make_incrementalsort_from_pathkeys function provides a high-level interface for creating IncrementalSort plan nodes from pathkey specifications. Similar to make_sort_from_pathkeys, it combines the functionality of prepare_sort_from_pathkeys and make_incrementalsort into a single convenient interface, but specifically for incremental sorting scenarios.

This function is used when the planner determines that the input data is already sorted by some prefix of the desired sort columns, allowing for more efficient sorting by only sorting within groups that have the same values for the presorted columns. The nPresortedCols parameter specifies how many leading columns are already sorted.

## Parameters / Member Variables
- `lefttree`: The input plan node that provides tuples to be incrementally sorted
- `pathkeys`: List of PathKey objects specifying the desired sort order
- `relids`: Set of relation IDs passed to prepare_sort_from_pathkeys for equivalence class member matching
- `nPresortedCols`: The number of leading columns that are already sorted in the input

## Dependencies
- Functions called/Symbols referenced:
  - [prepare_sort_from_pathkeys](../p/prepare_sort_from_pathkeys.md) (to convert pathkeys into sort specification arrays)
  - [make_incrementalsort](make_incrementalsort.md) (to create the IncrementalSort plan node)
- Called from (representative examples):
  - [create_incrementalsort_plan](../c/create_incrementalsort_plan.md) (src/backend/optimizer/plan/createplan.c:2224)

## Notes and Other Information
- This is a static function within createplan.c, used internally by the planner
- Provides a simplified interface for creating incremental sorts from pathkeys
- Uses default parameters for prepare_sort_from_pathkeys: reqColIdx=NULL, adjust_tlist_in_place=false
- The nPresortedCols parameter must be less than the total number of sort columns
- IncrementalSort is a PostgreSQL 13+ optimization for partially sorted input data
- The function may modify the input plan tree by adding Result nodes if projection is needed
- Part of the planner's optimization strategy to leverage existing sort order in the data
- Located at src/backend/optimizer/plan/createplan.c:6382-6415