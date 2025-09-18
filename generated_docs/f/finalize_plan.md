# finalize_plan

## Location
src/backend/optimizer/plan/subselect.c: 2292 - 2889

## Overview
Recursively processes all nodes in a plan tree to compute external parameter dependencies (extParam) and all parameter dependencies (allParam) for each plan node.

## Definition
```c
static Bitmapset *finalize_plan(PlannerInfo *root, Plan *plan, int gather_param, Bitmapset *valid_params, Bitmapset *scan_params)
```

## Detailed Description
finalize_plan is the core recursive function that performs parameter finalization for PostgreSQL plan trees. It traverses the entire plan tree depth-first, computing parameter dependency information that is essential for proper plan execution, particularly in the context of subqueries and correlated queries.

The function computes two critical parameter sets for each plan node:
- extParam: Parameters that come from outside the current plan node (external dependencies)
- allParam: All parameters that the plan node and its entire subtree depend on

The function handles various plan node types with type-specific processing, including scan nodes, join nodes, aggregate nodes, and utility nodes. For each node type, it analyzes the node's expressions and child plans to determine parameter dependencies.

Special handling is provided for:
- InitPlans: Processes initialization plans to determine external and set parameters
- Parallel processing: Handles parallel-aware nodes and Gather/GatherMerge coordination
- Nested loops: Manages parameter passing between left and right child nodes
- Subqueries: Recursively processes subquery plans with proper parameter scoping
- EvalPlanQual: Supports EPQ mechanism through scan_params

## Parameters / Member Variables
- `root`: PlannerInfo structure containing planning context and global information
- `plan`: The Plan node to be processed (can be NULL)
- `gather_param`: Parameter ID from an ancestral Gather/GatherMerge node, or -1 if none
- `valid_params`: Set of parameter IDs that are valid to reference from outer plan levels
- `scan_params`: Set of parameter IDs to force scan nodes to reference (for EvalPlanQual support)

## Dependencies
- Functions called/Symbols referenced:
  - [finalize_primnode](finalize_primnode.md) (processes individual expressions)
  - [finalize_agg_primnode](finalize_agg_primnode.md) (processes aggregate expressions)
  - planner_subplan_get_plan
  - [find_base_rel](find_base_rel.md)
  - Various bitmap set manipulation functions (bms_add_members, bms_union, etc.)
  - [Node](../N/Node.md) type checking (nodeTag)
- Called from (representative examples):
  - [SS_finalize_plan](../S/SS_finalize_plan.md) (entry point)
  - [finalize_plan](finalize_plan.md) (recursive calls to child plans)

## Notes and Other Information
- Returns the computed allParam set for the given plan node
- The function is designed to handle all plan node types through a comprehensive switch statement
- Parameter validation ensures that plan nodes only reference parameters that are valid in their scope
- [InitPlan](../I/InitPlan.md) processing assumes SS_finalize_plan has already been run on referenced plans
- The function includes extensive comments about limitations in initPlan parameter handling
- Critical for proper execution of correlated subqueries and nested plan structures
- Located in src/backend/optimizer/plan/subselect.c (static function)