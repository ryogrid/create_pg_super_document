# fix_windowagg_condition_expr

## Location
src/backend/optimizer/plan/setrefs.c: 3391 - 3411

## Overview
Converts references in window aggregate run conditions by replacing any WindowFunc references with corresponding Var references from the subplan target list.

## Definition
```c
static List *
fix_windowagg_condition_expr(PlannerInfo *root,
                             List *runcondition,
                             indexed_tlist *subplan_itlist)
```

## Detailed Description
This function is part of PostgreSQL's query plan reference fixing mechanism during plan tree construction. It processes run condition expressions for window aggregates, specifically converting WindowFunc nodes to Var nodes that reference the corresponding WindowFunc entries in the subplan's target list. This transformation is necessary to ensure that condition expressions can properly reference windowing functions that have been computed in lower plan nodes.

The function uses a mutator pattern to recursively traverse the condition expression tree and replace WindowFunc nodes with appropriate Var references. This is critical for the proper execution of window aggregate operations where conditions need to reference computed window function results.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing planner state and context information
- `runcondition`: List of condition expressions that may contain WindowFunc references to be converted
- `subplan_itlist`: Indexed target list from the subplan containing the WindowFunc entries to reference

## Dependencies
- Functions called/Symbols referenced:
  - fix_windowagg_condition_expr_mutator
  - fix_windowagg_cond_context (structure)
  - indexed_tlist (type)
- Called from (representative examples):
  - set_windowagg_runcondition_references

## Notes and Other Information
- This is a static function within the setrefs.c module, indicating it's used internally for plan reference fixing
- The function creates a context structure to pass state information to the recursive mutator function
- Part of the broader plan tree reference fixing process that ensures all variable references are properly resolved after plan construction
- Critical for window aggregate optimization and execution correctness