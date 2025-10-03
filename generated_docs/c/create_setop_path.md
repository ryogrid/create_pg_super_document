# create_setop_path

## Location
[src/backend/optimizer/util/pathnode.c:3555-3616](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/pathnode.c#L3555-L3616)

## Overview
Creates a pathnode that represents computation of INTERSECT or EXCEPT set operations, supporting both sorted and hashed implementation strategies.

## Definition

```c
SetOpPath *
create_setop_path(PlannerInfo *root,
				  RelOptInfo *rel,
				  Path *subpath,
				  SetOpCmd cmd,
				  SetOpStrategy strategy,
				  List *distinctList,
				  AttrNumber flagColIdx,
				  int firstFlag,
				  double numGroups,
				  double outputRows)
```
## Detailed Description
This function creates a SetOpPath node that represents the execution of set operations like INTERSECT and EXCEPT (with or without ALL). The function supports two implementation strategies: sorted and hashed. In sorted mode, the input must already be sorted and the output preserves the sort order. In hashed mode, no particular input ordering is required. The cost calculation assumes one cpu_operator_cost per comparison per input tuple across all columns in the distinctList. SetOp operations don't project new columns, so they reuse the source path's pathtarget.

## Parameters / Member Variables
- `*root`: PlannerInfo structure containing planner state and configuration
- `*rel`: RelOptInfo representing the parent relation associated with the result
- `*subpath`: Path representing the source of input data
- `cmd`: SetOpCmd enum specifying the specific semantics (INTERSECT or EXCEPT, with/without ALL)
- `strategy`: SetOpStrategy enum indicating implementation strategy (sorted or hashed)
- `*distinctList`: List of SortGroupClause structures representing the grouping columns
- `flagColIdx`: AttrNumber indicating the column number where the flag column will be placed, if any
- `firstFlag`: Integer flag value for the first input relation when using hashing strategy, or -1 when sorting
- `numGroups`: Double representing the estimated number of distinct groups
- `outputRows`: Double representing the estimated number of output rows
## Dependencies
- Functions called/Symbols referenced:
  - makeNode
  - [list_length](../l/list_length.md)
  - cpu_operator_cost
- Called from (representative examples):
  - [generate_nonunion_paths](../g/generate_nonunion_paths.md) (src/backend/optimizer/prep/prepunion.c:1179)

## Notes and Other Information
- [SetOp](../S/SetOp.md) operations don't project new columns, so they reuse the source path's pathtarget
- Assumes no parameterization (above any joins) for simplification
- Parallel safety depends on the relation's consider_parallel flag and subpath's parallel safety
- Preserves input sort order only when using SETOP_SORTED strategy
- Cost calculation charges one cpu_operator_cost per comparison per input tuple
- Assumes all columns in distinctList get compared for most tuples during execution
- The flagColIdx and firstFlag parameters are used for distinguishing between different input relations in the implementation
- Both INTERSECT and EXCEPT operations can be performed with or without the ALL keyword, affecting duplicate handling

## Simplified Source

```c
SetOpPath *
create_setop_path(PlannerInfo *root,
                  RelOptInfo *rel,
                  Path *subpath,
                  SetOpCmd cmd,
                  SetOpStrategy strategy,
                  List *distinctList,
                  AttrNumber flagColIdx,
                  int firstFlag,
                  double numGroups,
                  double outputRows)
{
    SetOpPath *pathnode = makeNode(SetOpPath);

    // Initialize basic path properties
    pathnode->path.pathtype = T_SetOp;
    pathnode->path.parent = rel;
    pathnode->path.pathtarget = subpath->pathtarget;  // No projection
    pathnode->path.param_info = NULL;                 // No parameterization
    pathnode->path.parallel_aware = false;
    pathnode->path.parallel_safe = rel->consider_parallel && subpath->parallel_safe;
    pathnode->path.parallel_workers = subpath->parallel_workers;

    // Preserve sort order only for sorted strategy
    pathnode->path.pathkeys = (strategy == SETOP_SORTED) ? subpath->pathkeys : NIL;

    // Set SetOp-specific properties
    pathnode->subpath = subpath;
    pathnode->cmd = cmd;
    pathnode->strategy = strategy;
    pathnode->distinctList = distinctList;
    pathnode->flagColIdx = flagColIdx;
    pathnode->firstFlag = firstFlag;
    pathnode->numGroups = numGroups;

    // Calculate costs (comparison cost per tuple)
    pathnode->path.startup_cost = subpath->startup_cost;
    pathnode->path.total_cost = subpath->total_cost +
        cpu_operator_cost * subpath->rows * list_length(distinctList);
    pathnode->path.rows = outputRows;

    return pathnode;
}
```