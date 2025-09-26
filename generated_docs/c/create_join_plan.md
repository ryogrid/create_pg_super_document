# create_join_plan

## Location
[src/backend/optimizer/plan/createplan.c:1082-1140](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/createplan.c#L1082-L1140)

## Overview
Creates execution plans for join operations by dispatching to specific join type implementations and optionally adding gating logic for pseudoconstant qualifiers.

## Definition
```c
static Plan *create_join_plan(PlannerInfo *root, JoinPath *best_path)
```

## Detailed Description
The `create_join_plan` function serves as the main dispatcher for creating join execution plans in PostgreSQL. It examines the path type of the best join path selected by the optimizer and delegates to the appropriate specialized function for creating merge joins, hash joins, or nested loop joins. After creating the basic join plan, it checks for pseudoconstant clauses that can be used for gating and wraps the plan with a Result node if needed. This function is a key component in translating the optimizer's path-based representation into executable plan nodes.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing global information about the query being planned
- `best_path`: JoinPath structure representing the selected join method and its parameters

## Dependencies
- Functions called/Symbols referenced:
  - [create_mergejoin_plan](create_mergejoin_plan.md)
  - [create_hashjoin_plan](create_hashjoin_plan.md)  
  - [create_nestloop_plan](create_nestloop_plan.md)
  - [get_gating_quals](../g/get_gating_quals.md)
  - [create_gating_plan](create_gating_plan.md)
  - [MergePath](../M/MergePath.md) (type)
  - [HashPath](../H/HashPath.md) (type) 
  - [NestPath](../N/NestPath.md) (type)
- Called from (representative examples):
  - [create_plan_recurse](create_plan_recurse.md)

## Notes and Other Information
- Uses a switch statement to dispatch based on path type (T_MergeJoin, T_HashJoin, T_NestLoop)
- Automatically adds gating Result nodes when pseudoconstant clauses are present in joinrestrictinfo
- Contains disabled code (under #ifdef NOT_USED) for handling expensive function pullups
- Will throw an ERROR for unrecognized path types to catch programming errors
- The function is static, meaning it's only used within the createplan.c compilation unit
- Gating clauses provide an optimization opportunity by allowing early termination of expensive join operations