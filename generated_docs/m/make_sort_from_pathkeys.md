# make_sort_from_pathkeys

## Location
[src/backend/optimizer/plan/createplan.c:6347-6381](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/createplan.c#L6347-L6381)

## Overview
The make_sort_from_pathkeys function creates a Sort plan node based on pathkey specifications, combining the functionality of prepare_sort_from_pathkeys and make_sort into a single convenient interface.

## Definition
```c
static Sort *
make_sort_from_pathkeys(Plan *lefttree, List *pathkeys, Relids relids)
```

## Detailed Description
The make_sort_from_pathkeys function provides a high-level interface for creating Sort plan nodes from pathkey specifications. It acts as a wrapper that combines two lower-level operations: first calling prepare_sort_from_pathkeys to convert the pathkeys into executor-ready sort specification arrays, and then calling make_sort to create the actual Sort plan node.

This function simplifies the process of creating sorts in cases where the caller has pathkeys but doesn't need the fine-grained control provided by prepare_sort_from_pathkeys. It handles the standard case where no specific column requirements exist and the targetlist can be modified by adding additional plan nodes if needed.

## Parameters / Member Variables
- `lefttree`: The input plan node that provides tuples to be sorted
- `pathkeys`: List of PathKey objects specifying the desired sort order
- `relids`: Set of relation IDs passed to prepare_sort_from_pathkeys for equivalence class member matching

## Dependencies
- Functions called/Symbols referenced:
  - [prepare_sort_from_pathkeys](../p/prepare_sort_from_pathkeys.md) (to convert pathkeys into sort specification arrays)
  - [make_sort](make_sort.md) (to create the Sort plan node)
- Called from (representative examples):
  - [create_sort_plan](../c/create_sort_plan.md) (src/backend/optimizer/plan/createplan.c:2200)
  - [create_mergejoin_plan](../c/create_mergejoin_plan.md) (src/backend/optimizer/plan/createplan.c:4530, 4544)

## Notes and Other Information
- This is a static function within createplan.c, used internally by the planner
- Provides a simplified interface compared to calling prepare_sort_from_pathkeys and make_sort separately
- Uses default parameters for prepare_sort_from_pathkeys: reqColIdx=NULL, adjust_tlist_in_place=false
- The function may modify the input plan tree by adding Result nodes if projection is needed
- Commonly used when creating sorts for merge joins and explicit sort operations
- Part of the higher-level plan creation interface that abstracts away sort array management
- Located at src/backend/optimizer/plan/createplan.c:6347-6381