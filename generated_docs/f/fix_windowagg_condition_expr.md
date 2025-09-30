# fix_windowagg_condition_expr

## Location
[src/backend/optimizer/plan/setrefs.c:3391-3411](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/setrefs.c#L3391-L3411)

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
  - [fix_windowagg_condition_expr_mutator](fix_windowagg_condition_expr_mutator.md)
  - [fix_windowagg_cond_context](fix_windowagg_cond_context.md) (structure)
  - [indexed_tlist](../i/indexed_tlist.md) (type)
- Called from (representative examples):
  - [set_windowagg_runcondition_references](../s/set_windowagg_runcondition_references.md)

## Notes and Other Information
- This is a static function within the setrefs.c module, indicating it's used internally for plan reference fixing
- The function creates a context structure to pass state information to the recursive mutator function
- Part of the broader plan tree reference fixing process that ensures all variable references are properly resolved after plan construction
- Critical for window aggregate optimization and execution correctness

## Simplified Source

```c
static List *
fix_windowagg_condition_expr(PlannerInfo *root,
                           List *runcondition,
                           indexed_tlist *subplan_itlist)
{
    fix_windowagg_cond_context context;

    // Setup context for mutator function
    context.root = root;
    context.subplan_itlist = subplan_itlist;
    context.newvarno = 0;

    // Use mutator to recursively convert WindowFunc references to Vars
    return (List *) fix_windowagg_condition_expr_mutator((Node *) runcondition,
                                                        &context);
}
```