# subpath_is_hashable

## Location
src/backend/optimizer/plan/subselect.c: 736 - 760

## Overview
Determines whether an ANY subplan can be implemented using hash-based execution by checking if the subquery result size fits within available hash memory limits, working from a Path instead of a Plan.

## Definition
```c
static bool subpath_is_hashable(Path *path)
```

## Detailed Description
The `subpath_is_hashable` function performs the same hashability evaluation as `subplan_is_hashable`, but operates on a Path structure instead of a Plan structure. This allows the hashability check to be performed earlier in the planning process, during path generation rather than after plan creation.

Like its Plan-based counterpart, this function calculates the estimated memory footprint of the subquery result by multiplying the estimated number of rows by the estimated row width, including tuple header overhead. The function uses the Path's cost estimates (rows and pathtarget width) to determine memory requirements.

The same conservative approach is used for memory estimation, employing heap tuple overhead despite actual storage using MinimalTuples, which provides a safety margin for hashtable overhead. If the estimated size exceeds the hash memory limit (hash_mem configuration parameter), hash-based execution is deemed infeasible.

## Parameters / Member Variables
- `path`: The execution path representing the subquery whose hashability is being evaluated

## Dependencies
- Functions called/Symbols referenced:
  - [get_hash_memory_limit](../g/get_hash_memory_limit.md) (returns the hash_mem limit)
  - `SizeofHeapTupleHeader` (constant for tuple header size)
  - `MAXALIGN` (macro for memory alignment)
- Called from (representative examples):
  - [make_subplan](../m/make_subplan.md) (src/backend/optimizer/plan/subselect.c:280)

## Notes and Other Information
This function is functionally identical to `subplan_is_hashable` but operates during the path-generation phase of query planning rather than the plan-creation phase. This earlier evaluation allows the optimizer to make decisions about subplan execution strategy before committing to a specific plan structure. The function accesses row estimates via `path->rows` and width estimates via `path->pathtarget->width`, which are the Path equivalents of Plan's `plan_rows` and `plan_width` fields. This dual implementation (Path vs Plan) is common in PostgreSQL's optimizer where similar checks need to be performed at different planning stages.