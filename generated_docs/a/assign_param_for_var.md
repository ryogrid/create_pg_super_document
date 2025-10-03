# assign_param_for_var

## Location
[src/backend/optimizer/util/paramassign.c:66-119](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/paramassign.c#L66-L119)

## Overview
Selects a PARAM_EXEC number to identify the given Var as a parameter for the current subquery and records the need for the Var in the proper upper-level root->plan_params.

## Definition

```c
static int
assign_param_for_var(PlannerInfo *root, Var *var)
```
## Detailed Description
This function is responsible for parameter assignment during query planning in PostgreSQL's optimizer. It handles the conversion of Var nodes into parameters that can be passed between query levels in nested subqueries. The function first searches for an existing matching PlannerParamItem to avoid creating duplicates, and if none is found, creates a new parameter entry.

The function navigates up the planner hierarchy to find the appropriate query level where the Var belongs, then either reuses an existing parameter or creates a new one. This is crucial for proper handling of correlated subqueries where variables from outer query levels need to be parameterized.

## Parameters / Member Variables
- `*root`: PlannerInfo structure containing planner state and context for the current query level
- `*var`: Var node representing a table column reference that needs to be parameterized
## Dependencies
- Functions called/Symbols referenced:
  - [PlannerParamItem](../P/PlannerParamItem.md) (structure creation)
  - [bms_equal](../b/bms_equal.md) (bitmap set equality comparison)
  - copyObject (deep copy of the Var node)
  - makeNode (node creation)
  - [lappend_oid](../l/lappend_oid.md) (append OID to list)
- Called from (representative examples):
  - [replace_outer_var](../r/replace_outer_var.md)

## Notes and Other Information
- The function performs a comparison that matches _equalVar() except for ignoring varlevelsup
- It ignores varnosyn, varattnosyn, and location fields during comparison
- The copied Var has its varlevelsup reset to 0 since it will be used as a parameter
- Parameter IDs are assigned sequentially based on the length of glob->paramExecTypes
- This is a static function within paramassign.c, indicating it's used internally for parameter assignment logic

## Simplified Source

```c
static int
assign_param_for_var(PlannerInfo *root, Var *var)
{
    ListCell *ppl;
    PlannerParamItem *pitem;
    Index levelsup;

    // Navigate to the query level where this Var belongs
    for (levelsup = var->varlevelsup; levelsup > 0; levelsup--)
        root = root->parent_root;

    // Check if we already have a parameter for this Var
    foreach(ppl, root->plan_params)
    {
        pitem = (PlannerParamItem *) lfirst(ppl);
        if (IsA(pitem->item, Var))
        {
            Var *existing_var = (Var *) pitem->item;

            // Compare Var fields (matches _equalVar() except varlevelsup)
            if (existing_var->varno == var->varno &&
                existing_var->varattno == var->varattno &&
                existing_var->vartype == var->vartype &&
                existing_var->vartypmod == var->vartypmod &&
                existing_var->varcollid == var->varcollid &&
                bms_equal(existing_var->varnullingrels, var->varnullingrels))
                return pitem->paramId;
        }
    }

    // Create new parameter entry for this Var
    var = copyObject(var);
    var->varlevelsup = 0;  // Reset since it becomes a parameter

    // Create and initialize new parameter item
    pitem = makeNode(PlannerParamItem);
    pitem->item = (Node *) var;
    pitem->paramId = list_length(root->glob->paramExecTypes);

    // Record parameter type and add to lists
    root->glob->paramExecTypes = lappend_oid(root->glob->paramExecTypes,
                                            var->vartype);
    root->plan_params = lappend(root->plan_params, pitem);

    return pitem->paramId;
}
```