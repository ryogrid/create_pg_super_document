# create_lockrows_path

## Location
[src/backend/optimizer/util/pathnode.c:3662-3724](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/pathnode.c#L3662-L3724)

## Overview
Creates a pathnode that represents acquiring row locks on tuples from a subpath, used in query planning to handle SELECT FOR UPDATE and similar locking operations.

## Definition


## Detailed Description
This function creates a LockRowsPath node that represents the operation of acquiring row locks during query execution. It's primarily used for implementing SELECT FOR UPDATE, SELECT FOR SHARE, and similar locking constructs. The function wraps an existing subpath and adds the necessary metadata for row locking operations.

The function initializes a LockRowsPath structure with appropriate cost estimates and properties. It assumes the operation is above any joins and therefore doesn't require parameterization. The resulting path cannot maintain any sort order since locking operations may cause sort key columns to be replaced with new values.

Cost estimation includes the base cost from the subpath plus an additional cpu_tuple_cost per row to account for the overhead of row locking and possible tuple refetches during EvalPlanQual processing.

## Parameters / Member Variables
- : PlannerInfo structure containing global planning information
- : RelOptInfo representing the parent relation associated with the result
- : Path representing the source of data that will have locks applied
- : List of PlanRowMark structures specifying the locking requirements
- : Parameter ID used for EvalPlanQual re-evaluation when concurrent updates occur

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (to create LockRowsPath node)
  - cpu_tuple_cost (cost constant for tuple processing)
- Called from (representative examples):
  - [grouping_planner](../g/grouping_planner.md) (src/backend/optimizer/plan/planner.c:1805)

## Notes and Other Information
- The resulting path has no pathkeys (sort order) since locking may modify sort key values
- Parallel execution is disabled for lock rows operations (parallel_aware = false, parallel_safe = false)
- Uses the same pathtarget as the subpath since LockRows doesn't project new columns
- Cost estimation is somewhat conservative, charging cpu_tuple_cost per row for locking overhead
- [EvalPlanQual](../E/EvalPlanQual.md) mechanism is supported through the epqParam for handling concurrent modifications