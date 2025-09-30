# pull_varnos_walker

## Location
[src/backend/optimizer/util/var.c:155-290](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/var.c#L155-L290)

## Overview
The core walker function that traverses expression trees to collect variable range table numbers (varnos), handling different node types including Vars, PlaceHolderVars, and CurrentOfExpr nodes.

## Definition
```c
static bool pull_varnos_walker(Node *node, pull_varnos_context *context)
```

## Detailed Description
The `pull_varnos_walker` function is the workhorse of the varno extraction system in PostgreSQL's query planner. It implements a tree walker pattern that recursively traverses expression trees and collects variable range table numbers based on the target sublevel specified in the context.

The function handles several node types specially:

1. **Var nodes**: Extracts the varno and any nulling relations if the variable is at the target sublevel
2. **CurrentOfExpr nodes**: Adds the cursor variable number for level-zero queries
3. **PlaceHolderVar nodes**: Complex handling that considers evaluation contexts, with special logic for translated appendrel PHVs
4. **Query nodes**: Recursively processes subqueries with appropriate level adjustment

The PlaceHolderVar handling is particularly sophisticated, dealing with ph_eval_at computation, appendrel translation, and fallback scenarios when PlaceHolderInfo is not yet available.

## Parameters / Member Variables
- `node`: The current Node being examined in the tree traversal
- `context`: Walker context containing:
  - `varnos`: Accumulating bitmap of discovered varnos
  - `root`: PlannerInfo for accessing PlaceHolderInfo
  - `sublevels_up`: Target query nesting level

## Dependencies
- Functions called/Symbols referenced:
  - [bms_add_member](../b/bms_add_member.md), bms_add_members (bitmapset operations)
  - [bms_equal](../b/bms_equal.md), bms_difference, bms_join (bitmapset comparisons/operations)
  - query_tree_walker, expression_tree_walker (tree traversal)
  - [pull_varnos_context](pull_varnos_context.md) (walker context structure)
  - [CurrentOfExpr](../C/CurrentOfExpr.md), PlaceHolderVar, PlaceHolderInfo (node types)
- Called from (representative examples):
  - [pull_varnos](pull_varnos.md)
  - [pull_varnos_of_level](pull_varnos_of_level.md)
  - [pull_varnos_walker](pull_varnos_walker.md) (recursive calls)

## Notes and Other Information
- Implements the visitor pattern for expression tree traversal
- Handles complex PlaceHolderVar scenarios including appendrel translation
- Uses bitmapset operations for efficient varno collection
- Properly manages query nesting levels through sublevels_up tracking
- Returns false to continue traversal, except when PlaceHolderVar processing is complete
- Critical component of PostgreSQL's query planning infrastructure for variable analysis

## Simplified Source

```c
// Simplified version of pull_varnos_walker
static bool
pull_varnos_walker(Node *node, pull_varnos_context *context)
{
    if (node == NULL)
        return false;

    // Extract varno from Var nodes at target sublevel
    if (IsA(node, Var))
    {
        Var *var = (Var *) node;
        if (var->varlevelsup == context->sublevels_up)
        {
            context->varnos = bms_add_member(context->varnos, var->varno);
            context->varnos = bms_add_members(context->varnos, var->varnullingrels);
        }
        return false;
    }

    // Handle CurrentOfExpr for cursor variables
    if (IsA(node, CurrentOfExpr))
    {
        CurrentOfExpr *cexpr = (CurrentOfExpr *) node;
        if (context->sublevels_up == 0)
            context->varnos = bms_add_member(context->varnos, cexpr->cvarno);
        return false;
    }

    // Handle PlaceHolderVar - complex evaluation logic
    if (IsA(node, PlaceHolderVar))
    {
        PlaceHolderVar *phv = (PlaceHolderVar *) node;

        if (phv->phlevelsup == context->sublevels_up && context->root != NULL)
        {
            PlaceHolderInfo *phinfo = NULL;

            // Try to get PlaceHolderInfo for better evaluation
            if (phv->phlevelsup == 0 && phv->phid < context->root->placeholder_array_size)
                phinfo = context->root->placeholder_array[phv->phid];

            if (phinfo == NULL)
            {
                // Fallback: use phrels from PHV itself
                context->varnos = bms_add_members(context->varnos, phv->phrels);
            }
            else if (bms_equal(phv->phrels, phinfo->ph_var->phrels))
            {
                // Normal case: use computed evaluation set
                context->varnos = bms_add_members(context->varnos, phinfo->ph_eval_at);
            }
            else
            {
                // Translated PlaceHolderVar: adjust evaluation set
                Relids delta = bms_difference(phinfo->ph_var->phrels, phv->phrels);
                Relids newevalat = bms_difference(phinfo->ph_eval_at, delta);

                if (!bms_equal(newevalat, phinfo->ph_eval_at))
                {
                    delta = bms_difference(phv->phrels, phinfo->ph_var->phrels);
                    newevalat = bms_join(newevalat, delta);
                }
                context->varnos = bms_join(context->varnos, newevalat);
            }

            // Add nulling relations
            context->varnos = bms_add_members(context->varnos, phv->phnullingrels);
            return false; // Don't recurse into expression
        }
    }

    // Handle subqueries with level adjustment
    if (IsA(node, Query))
    {
        context->sublevels_up++;
        bool result = query_tree_walker((Query *) node, pull_varnos_walker,
                                      (void *) context, 0);
        context->sublevels_up--;
        return result;
    }

    // Continue tree traversal for other node types
    return expression_tree_walker(node, pull_varnos_walker, (void *) context);
}
```