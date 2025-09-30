# find_placeholder_info

## Location
[src/backend/optimizer/util/placeholder.c:83-184](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/placeholder.c#L83-L184)

## Overview
Retrieves or creates a PlaceHolderInfo structure for a given PlaceHolderVar, managing the metadata needed for proper evaluation placement and optimization of placeholder expressions in query plans.

## Definition
```c
PlaceHolderInfo *find_placeholder_info(PlannerInfo *root, PlaceHolderVar *phv)
```

## Detailed Description
The `find_placeholder_info` function is responsible for finding or creating PlaceHolderInfo structures, which contain essential metadata about where and how PlaceHolderVars should be evaluated during query execution. The function first attempts to locate an existing PlaceHolderInfo using a fast array lookup. If not found, it creates a new one, analyzing the expressions referenced variables to determine evaluation placement, lateral references, and other optimization parameters.

The function performs sophisticated analysis to separate LATERAL references (variables outside the PHVs syntactic scope) from evaluation requirements, and handles dynamic memory allocation for the placeholder_array as needed. It also recursively processes any nested PlaceHolderVars within the expression.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing query planning context and placeholder management structures
- `phv`: PlaceHolderVar for which to find or create the corresponding PlaceHolderInfo

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (for creating PlaceHolderInfo)
  - copyObject (for copying PlaceHolderVar)
  - [pull_varnos](../p/pull_varnos.md) (for extracting variable references)
  - [bms_difference](../b/bms_difference.md), bms_int_members, bms_is_empty, bms_copy (bitmap set operations)
  - [get_typavgwidth](../g/get_typavgwidth.md), exprType, exprTypmod (type analysis functions)
  - repalloc0_array, palloc0_array (memory management)
  - [find_placeholders_in_expr](find_placeholders_in_expr.md) (recursive placeholder processing)
- Called from (representative examples):
  - [set_rel_width](../s/set_rel_width.md) (in costsize.c)
  - [replace_nestloop_params_mutator](../r/replace_nestloop_params_mutator.md) (in createplan.c)
  - [add_vars_to_targetlist](../a/add_vars_to_targetlist.md) (in initsplan.c)
  - build_joinrel_tlist (in relnode.c)

## Notes and Other Information
- Only callable after query_planner() has started due to placeholder freezing constraints
- Uses both placeholder_list (for iteration) and placeholder_array (for fast lookup) data structures
- Dynamically expands placeholder_array using exponential growth when needed
- Separates ph_lateral (LATERAL references) from ph_eval_at (evaluation requirements)
- Forces evaluation at syntactic location if no contained variables are found within scope
- Recursively processes nested PlaceHolderVars in the expression
- Throws error if called after placeholders are frozen (too late in planning process)

## Simplified Source

```c
PlaceHolderInfo *find_placeholder_info(PlannerInfo *root, PlaceHolderVar *phv)
{
    PlaceHolderInfo *phinfo;
    Relids rels_used;

    // Quick lookup using placeholder array
    if (phv->phid < root->placeholder_array_size)
        phinfo = root->placeholder_array[phv->phid];
    else
        phinfo = NULL;

    if (phinfo != NULL)
        return phinfo;

    // Not found, create new PlaceHolderInfo
    if (root->placeholdersFrozen)
        elog(ERROR, "too late to create a new PlaceHolderInfo");

    phinfo = makeNode(PlaceHolderInfo);
    phinfo->phid = phv->phid;
    phinfo->ph_var = copyObject(phv);
    phinfo->ph_var->phnullingrels = NULL; // Clear nulling rels

    // Analyze variable references to determine evaluation placement
    rels_used = pull_varnos(root, (Node *) phv->phexpr);
    phinfo->ph_lateral = bms_difference(rels_used, phv->phrels);
    phinfo->ph_eval_at = bms_int_members(rels_used, phv->phrels);

    // Force evaluation at syntactic location if no contained vars
    if (bms_is_empty(phinfo->ph_eval_at)) {
        phinfo->ph_eval_at = bms_copy(phv->phrels);
    }

    phinfo->ph_needed = NULL; // Initially unused
    phinfo->ph_width = get_typavgwidth(exprType((Node *) phv->phexpr),
                                      exprTypmod((Node *) phv->phexpr));

    // Add to placeholder list
    root->placeholder_list = lappend(root->placeholder_list, phinfo);

    // Expand placeholder array if needed
    if (phinfo->phid >= root->placeholder_array_size) {
        int new_size = root->placeholder_array_size ?
                      root->placeholder_array_size * 2 : 8;
        while (phinfo->phid >= new_size)
            new_size *= 2;

        if (root->placeholder_array)
            root->placeholder_array = repalloc0_array(root->placeholder_array,
                                                     PlaceHolderInfo *,
                                                     root->placeholder_array_size,
                                                     new_size);
        else
            root->placeholder_array = palloc0_array(PlaceHolderInfo *, new_size);

        root->placeholder_array_size = new_size;
    }
    root->placeholder_array[phinfo->phid] = phinfo;

    // Process nested PlaceHolderVars
    find_placeholders_in_expr(root, (Node *) phinfo->ph_var->phexpr);

    return phinfo;
}
```