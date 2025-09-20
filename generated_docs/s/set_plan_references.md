# set_plan_references

## Location
[src/backend/optimizer/plan/setrefs.c:287-390](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/setrefs.c#L287-L390)

## Overview
The final processing pass of the planner/optimizer that adjusts representational details in the plan tree for the convenience of the executor.

## Definition

```c
Plan *
set_plan_references(PlannerInfo *root, Plan *plan)
```
## Detailed Description
The  function performs the final transformation phase of PostgreSQL's query planner. It takes a complete plan tree and adjusts various representational details to prepare it for execution. The function performs nine key operations:

1. **Rangetable flattening**: Consolidates various subquery rangetables into a single flat list and clears unnecessary RangeTblEntry fields
2. **Scan node Var adjustment**: Updates variable references in scan nodes to be consistent with the flattened rangetable
3. **Upper node Var adjustment**: Modifies variable references in upper plan nodes to refer to outputs of their subplans
4. **Aggref adjustment**: Updates aggregate references in Agg plan nodes for cases involving partial aggregation or minmax optimization
5. **PARAM_MULTIEXPR replacement**: Converts PARAM_MULTIEXPR parameters to regular PARAM_EXEC parameters after planning MULTIEXPR subplans
6. **AlternativeSubPlan resolution**: Replaces AlternativeSubPlan expressions with single alternatives based on execution estimates
7. **Operator OID computation**: Looks up function implementations for operators (regproc OIDs)
8. **Dependency tracking**: Creates lists of objects the plan depends on for cache invalidation purposes
9. **Plan node ID assignment**: Assigns unique IDs to every plan node in the tree

Additionally, the function performs a final optimization by removing unnecessary SubqueryScan, Append, and MergeAppend nodes that don't serve a useful purpose after reference setting.

## Parameters / Member Variables
- : PlannerInfo structure containing planning context and global information
- : The root Plan node of the plan tree to process

## Dependencies
- Functions called/Symbols referenced:
  - [add_rtes_to_flat_rtable](../a/add_rtes_to_flat_rtable.md)
  - [set_plan_refs](set_plan_refs.md)
  - foreach_current_index
- Types used:
  - PlannerGlobal
  - PlanRowMark
  - [AppendRelInfo](../A/AppendRelInfo.md)
- Called from (representative examples):
  - [standard_planner](standard_planner.md)
  - [set_subqueryscan_references](set_subqueryscan_references.md)

## Notes and Other Information
- Modifies Plan nodes in-place but uses expression_tree_mutator for targetlist and qual expressions
- The function assumes Plan nodes are newly built and not multiply referenced
- Returns the same Plan node passed in, except when the input node is deemed unnecessary
- Results are stored in various global lists: finalrtable, finalrowmarks, resultRelations, appendRelations, relationOids, and invalItems
- Handles AlternativeSubPlans by maintaining workspace arrays (isAltSubplan, isUsedSubplan) to track usage