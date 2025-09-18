# extract_actual_clauses

## Location
[src/backend/optimizer/util/restrictinfo.c:494-521](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/restrictinfo.c#L494-L521)

## Overview
Extracts bare clause expressions from a list of RestrictInfo structures, with selective filtering based on pseudoconstant status and automatic elimination of constant-TRUE clauses.

## Definition
```c
List *extract_actual_clauses(List *restrictinfo_list, bool pseudoconstant)
```

## Detailed Description
This function provides a flexible mechanism for extracting clause expressions from RestrictInfo wrappers with built-in filtering capabilities. It allows callers to selectively extract either regular clauses or pseudoconstant clauses based on the boolean parameter, while automatically filtering out constant-TRUE clauses that would be redundant in the final execution plan.

The function performs two levels of filtering: first, it checks whether each RestrictInfo's pseudoconstant flag matches the requested type, and second, it eliminates any clauses that represent constant TRUE values. This dual filtering ensures that only meaningful, executable clauses are included in the result.

## Parameters / Member Variables
- `restrictinfo_list`: Input list of RestrictInfo pointers from which to extract clauses
- `pseudoconstant`: Boolean flag indicating whether to extract pseudoconstant clauses (true) or regular clauses (false)

## Dependencies
- Functions called/Symbols referenced:
  - [rinfo_is_constant_true](../r/rinfo_is_constant_true.md) (Line 505) - to filter out constant TRUE clauses
  - lfirst_node macro - for safe list iteration
  - lappend - to build the result list
  - NIL - PostgreSQL's empty list constant
- Called from (representative examples):
  - [get_gating_quals](../g/get_gating_quals.md) (src/backend/optimizer/plan/createplan.c:1013)
  - [create_seqscan_plan](../c/create_seqscan_plan.md) (src/backend/optimizer/plan/createplan.c:2931)
  - [create_indexscan_plan](../c/create_indexscan_plan.md) (src/backend/optimizer/plan/createplan.c:3095)
  - [create_nestloop_plan](../c/create_nestloop_plan.md) (src/backend/optimizer/plan/createplan.c:4405)
  - [create_mergejoin_plan](../c/create_mergejoin_plan.md) (src/backend/optimizer/plan/createplan.c:4493)

## Notes and Other Information
- Unlike get_actual_clauses, this function performs active filtering rather than assuming clean input
- Pseudoconstant clauses are typically expressions that can be evaluated once and reused across multiple rows
- The function is extensively used throughout the plan creation process, appearing in most scan and join plan creation functions
- Constant-TRUE clause elimination helps optimize the final execution plan by removing redundant always-true conditions
- The selective extraction capability allows different parts of the planner to work with appropriate subsets of available clauses