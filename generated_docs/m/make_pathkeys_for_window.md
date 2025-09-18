# make_pathkeys_for_window

## Location
src/backend/optimizer/plan/planner.c: 6201 - 6327

## Overview
Creates a pathkeys list describing the required input ordering for a given WindowClause, combining PARTITION BY and ORDER BY requirements.

## Definition
```c
static List *
make_pathkeys_for_window(PlannerInfo *root, WindowClause *wc,
                        List *tlist)
```

## Detailed Description
This function creates the pathkeys list that describes the required input ordering for window functions. The required ordering is first the PARTITION keys, then the ORDER keys. The function validates that both PARTITION BY and ORDER BY clauses contain sortable datatypes and creates appropriate pathkeys for each.

The function modifies the WindowClause's partitionClause to remove any clauses which are deemed redundant by the pathkey logic. This optimization helps reduce unnecessary sorting operations. However, it does not remove redundant ORDER BY items because the executor needs the ordering column for RANGE OFFSET cases for in_range tests, even if it's known to be equal to some partitioning column.

Currently, windowing is implemented using sorting only. In the future, hashing might be considered for partitioning, which could relax the ordering requirements.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing query planning information
- `wc`: WindowClause specifying the window function's PARTITION BY and ORDER BY requirements
- `tlist`: Target list containing the expressions that can be referenced in the window clauses

## Dependencies
- Functions called/Symbols referenced:
  - [grouping_is_sortable](../g/grouping_is_sortable.md)
  - [make_pathkeys_for_sortclauses_extended](make_pathkeys_for_sortclauses_extended.md)
  - [make_pathkeys_for_sortclauses](make_pathkeys_for_sortclauses.md)
  - [append_pathkeys](../a/append_pathkeys.md)
- Called from:
  - [create_one_window_path](../c/create_one_window_path.md) (src/backend/optimizer/plan/planner.c:4699)
  - [standard_qp_callback](../s/standard_qp_callback.md) (src/backend/optimizer/plan/planner.c:3592)
  - standard_qp_extra (src/backend/optimizer/plan/planner.c:217)

## Notes and Other Information
- This is a static function within planner.c
- Throws ERROR if PARTITION BY or ORDER BY columns are not of sortable datatypes
- The function can safely remove redundant clauses from partitionClause but preserves all ORDER BY clauses
- The function assumes that windowing will always use sorting (no hashing implementation yet)
- Uses make_pathkeys_for_sortclauses_extended for PARTITION BY to allow redundancy removal
- Uses regular make_pathkeys_for_sortclauses for ORDER BY to preserve all clauses