# finalize_primnode_context

## Location
[src/backend/optimizer/plan/subselect.c:54-58](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/subselect.c#L54-L58)

## Overview
A context structure used during plan finalization to track and accumulate PARAM_EXEC parameter IDs that are referenced throughout the plan tree.

## Definition

```c
typedef struct finalize_primnode_context
{
	PlannerInfo *root;
	Bitmapset  *paramids;		/* Non-local PARAM_EXEC paramids found */
} finalize_primnode_context;
```
## Detailed Description
The  structure serves as a parameter context for the  function during the final phase of query plan preparation. This structure is used in PostgreSQL's query planner to identify and collect all PARAM_EXEC parameter IDs that appear in expression trees throughout the plan. The context maintains a running set of parameter IDs that represent cross-plan-node parameter dependencies, which is crucial for proper plan execution and parameter passing. This information is used to populate the  and  fields of plan nodes, enabling the executor to properly handle parameter-dependent operations like subqueries and joins.

## Parameters / Member Variables
- : PlannerInfo pointer containing the current planner state and global query planning context
- : A Bitmapset accumulating the IDs of all non-local PARAM_EXEC parameters found during the tree traversal, representing parameter dependencies that cross plan node boundaries

## Dependencies
- Functions called/Symbols referenced:
  - [PlannerInfo](../P/PlannerInfo.md) (structure)
  - [Bitmapset](../B/Bitmapset.md) (structure)
- Called from (representative examples):
  - [finalize_plan](finalize_plan.md) (src/backend/optimizer/plan/subselect.c:2297, 2471, 2738)
  - [finalize_primnode](finalize_primnode.md) (src/backend/optimizer/plan/subselect.c:2890)
  - [finalize_agg_primnode](finalize_agg_primnode.md) (src/backend/optimizer/plan/subselect.c:2974)

## Notes and Other Information
- This context structure follows PostgreSQL's expression tree walker pattern for systematic plan tree traversal
- The  bitmap accumulates parameter IDs across multiple finalize_primnode calls, building a complete picture of parameter dependencies
- Used extensively during plan finalization to identify which PARAM_EXEC parameters each plan node depends on
- Critical for proper execution of parameterized plans, subqueries, and nested loop joins
- The accumulated parameter IDs are used to set the  and  fields of plan nodes, which guide parameter propagation during execution
- Part of the SS_finalize_plan infrastructure that ensures plans are properly prepared for execution
- Distinguished from locally generated parameters, which are handled separately and excluded from cross-node dependencies