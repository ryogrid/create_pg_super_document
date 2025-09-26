# is_projection_capable_plan

## Location
[src/backend/optimizer/plan/createplan.c:7284-7319](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/createplan.c#L7284-L7319)

## Overview
Determines whether a given Plan node is capable of performing projection (selecting specific columns and computing expressions) as part of its execution.

## Definition
```c
bool
is_projection_capable_plan(Plan *plan)
```

## Detailed Description  
The `is_projection_capable_plan` function evaluates whether a specific plan node type can handle projection operations directly during execution. This is similar to `is_projection_capable_path` but operates on actual Plan nodes rather than Path nodes. The function uses a negative filtering approach, listing plan node types that cannot perform projection and returning false for those. Most plan types can project, so this approach is more concise. Special handling is provided for CustomScan nodes (which may support projection based on flags) and ProjectSet nodes (which are restricted to preserve set-returning function semantics).

## Parameters / Member Variables
- `plan`: Plan * - The plan node to check for projection capability

## Dependencies
- Functions called/Symbols referenced:
  - nodeTag (gets the node type tag for the plan)
  - [CustomScan](../C/CustomScan.md) (plan node type for custom scans)
  - CUSTOMPATH_SUPPORT_PROJECTION (flag indicating custom scan supports projection)
- Called from (representative examples):
  - [create_projection_plan](../c/create_projection_plan.md) (determines if separate projection node is needed)
  - [change_plan_targetlist](../c/change_plan_targetlist.md) (modifies plan target lists when possible)
  - [prepare_sort_from_pathkeys](../p/prepare_sort_from_pathkeys.md) (sorting preparation logic)

## Notes and Other Information
- Uses negative filtering approach - assumes most plans can project and lists exceptions
- Non-projection-capable plan types include: Hash, Material, Memoize, Sort, Unique, SetOp, LockRows, Limit, ModifyTable, Append, MergeAppend, RecursiveUnion
- Unlike the path version, all Append plans are considered non-projection-capable (no dummy path exception)
- [CustomScan](../C/CustomScan.md) plans can project only if they have the CUSTOMPATH_SUPPORT_PROJECTION flag set
- [ProjectSet](../P/ProjectSet.md) plans are considered non-projection-capable to prevent target list replacement that could interfere with set-returning functions
- This function is used during plan tree manipulation to determine if projection operations can be pushed down into existing nodes
- The restriction on ProjectSet may be relaxed in future PostgreSQL versions
- Closely related to `is_projection_capable_path` but operates on the execution plan rather than the planning path