# replace_nestloop_params_mutator

## Location
[src/backend/optimizer/plan/createplan.c:4943-5022](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/createplan.c#L4943-L5022)

## Overview
Performs recursive tree walking to replace outer-relation Vars and PlaceHolderVars with nestloop Params in expression trees.

## Definition

```c
structure with the original;
```
## Detailed Description
This is the core mutator function that implements the nested loop parameter replacement logic. It recursively traverses expression trees and replaces Vars and PlaceHolderVars that belong to outer relations with corresponding Params. The function handles two main node types:

**For Var nodes:**
- Checks if the Var belongs to an outer relation (identified by root->curOuterRels)
- Skips special varnos and variables not in outer relations
- Replaces qualifying Vars with nestloop Params using replace_nestloop_param_var()

**For PlaceHolderVar nodes:**
- Determines if the PHV needs to be replaced based on its evaluation context
- If the PHV can't be replaced entirely, it creates a flat copy and recursively processes its expression
- Replaces qualifying PHVs with nestloop Params using replace_nestloop_param_placeholdervar()

For all other node types, it delegates to expression_tree_mutator() to continue the recursive traversal.

## Parameters / Member Variables
- : The expression tree node to be processed (can be NULL)
- : PlannerInfo structure containing planner context, including curOuterRels and curOuterParams

## Dependencies
- Functions called/Symbols referenced:
  - IS_SPECIAL_VARNO
  - [bms_is_member](../b/bms_is_member.md)
  - [replace_nestloop_param_var](replace_nestloop_param_var.md)
  - [bms_is_subset](../b/bms_is_subset.md)
  - [find_placeholder_info](../f/find_placeholder_info.md)
  - [replace_nestloop_param_placeholdervar](replace_nestloop_param_placeholdervar.md)
  - expression_tree_mutator
  - [replace_nestloop_params_mutator](replace_nestloop_params_mutator.md) (recursive call)
- Called from (representative examples):
  - [replace_nestloop_params](replace_nestloop_params.md)
  - [replace_nestloop_params_mutator](replace_nestloop_params_mutator.md) (recursive calls)

## Notes and Other Information
This function is a critical component of PostgreSQL's nested loop join implementation. It ensures proper parameterization of outer relation references, which is essential for efficient nested loop execution. The function handles PlaceHolderVars specially, creating copies when the entire PHV cannot be replaced but its internal expressions need processing. The recursive nature allows it to handle complex nested expression structures while maintaining the correct parameter relationships. Located in src/backend/optimizer/plan/createplan.c at lines 4943-5022.

## Simplified Source

```c
static Node *
replace_nestloop_params_mutator(Node *node, PlannerInfo *root)
{
    if (node == NULL)
        return NULL;

    if (IsA(node, Var))
    {
        Var *var = (Var *) node;

        // Upper-level Vars should be resolved by now
        Assert(var->varlevelsup == 0);

        // Skip special varnos and vars not in outer relations
        if (IS_SPECIAL_VARNO(var->varno) ||
            !bms_is_member(var->varno, root->curOuterRels))
            return node;

        // Replace with nestloop parameter
        return (Node *) replace_nestloop_param_var(root, var);
    }

    if (IsA(node, PlaceHolderVar))
    {
        PlaceHolderVar *phv = (PlaceHolderVar *) node;

        // Upper-level PHVs should be resolved by now
        Assert(phv->phlevelsup == 0);

        // Check if PHV needs replacement based on evaluation context
        if (!bms_is_subset(find_placeholder_info(root, phv)->ph_eval_at,
                          root->curOuterRels))
        {
            // Can't replace whole PHV, but process its expression
            // Create a flat copy and recurse on the expression
            PlaceHolderVar *newphv = makeNode(PlaceHolderVar);

            memcpy(newphv, phv, sizeof(PlaceHolderVar));
            newphv->phexpr = (Expr *)
                replace_nestloop_params_mutator((Node *) phv->phexpr, root);
            return (Node *) newphv;
        }

        // Replace the entire PHV with a nestloop parameter
        return (Node *) replace_nestloop_param_placeholdervar(root, phv);
    }

    // For all other node types, continue recursive traversal
    return expression_tree_mutator(node,
                                  replace_nestloop_params_mutator,
                                  (void *) root);
}
```