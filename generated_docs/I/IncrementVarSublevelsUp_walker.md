# IncrementVarSublevelsUp_walker

## Location
[src/backend/rewrite/rewriteManip.c:777-849](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/rewrite/rewriteManip.c#L777-L849)

## Overview
A static walker function that recursively increments the sublevel counters in various PostgreSQL node types to adjust variable references for subquery nesting changes.

## Definition
```c
static bool IncrementVarSublevelsUp_walker(Node *node, IncrementVarSublevelsUp_context *context)
```

## Detailed Description
This walker function is responsible for adjusting sublevel references throughout an expression or query tree when the nesting depth of subqueries changes. It handles multiple PostgreSQL node types that track sublevel information including Var nodes (varlevelsup), aggregate functions (agglevelsup), grouping functions (agglevelsup), placeholder variables (phlevelsup), and CTE range table entries (ctelevelsup).

The function operates by comparing each node's current sublevel against a minimum threshold, and only increments levels that meet or exceed this threshold. This selective updating is crucial for correctly handling nested subquery structures where only certain levels should be affected.

The function includes special error handling for CurrentOfExpr nodes, which cannot be pushed down to lower subquery levels and will generate an error if encountered at the minimum sublevel.

## Parameters / Member Variables
- `node`: The current node being processed in the tree traversal
- `context`: An IncrementVarSublevelsUp_context structure containing:
  - `delta_sublevels_up`: The amount to increment qualifying sublevel counters
  - `min_sublevels_up`: The minimum sublevel threshold for applying increments

## Dependencies
- Functions called/Symbols referenced:
  - query_tree_walker
  - expression_tree_walker
  - IsA (macro for node type checking)
  - elog (for error reporting)
  - [QTW_EXAMINE_RTES_BEFORE](../Q/QTW_EXAMINE_RTES_BEFORE.md) (flag for query tree walking)
- Called from (representative examples):
  - [IncrementVarSublevelsUp](IncrementVarSublevelsUp.md) (recursive self-call through walker infrastructure)
  - [IncrementVarSublevelsUp_rtable](IncrementVarSublevelsUp_rtable.md) (for range table processing)
  - query_tree_walker (for subquery traversal)
  - expression_tree_walker (for expression traversal)

## Notes and Other Information
- This is a static function used only within rewriteManip.c
- Handles multiple node types: Var, CurrentOfExpr, Aggref, GroupingFunc, PlaceHolderVar, RangeTblEntry, and Query
- For Query nodes, increments min_sublevels_up before recursing and decrements afterward to maintain proper nesting context
- Uses QTW_EXAMINE_RTES_BEFORE flag to ensure range table entries are processed before the query body
- The function prevents inappropriate sublevel increments by checking against min_sublevels_up threshold
- Essential for query transformation operations that change subquery nesting structure
- Includes safety check for CurrentOfExpr nodes which cannot be moved between subquery levels
- Returns false in most cases to continue tree traversal, only stopping for terminal nodes like Var and CurrentOfExpr

## Simplified Source

```c
static bool IncrementVarSublevelsUp_walker(Node *node,
                                          IncrementVarSublevelsUp_context *context) {
    if (node == NULL) {
        return false;
    }

    // Handle Var nodes - increment varlevelsup if above threshold
    if (IsA(node, Var)) {
        Var *var = (Var *) node;
        if (var->varlevelsup >= context->min_sublevels_up) {
            var->varlevelsup += context->delta_sublevels_up;
        }
        return false;
    }

    // Handle CurrentOfExpr - error if trying to push down
    if (IsA(node, CurrentOfExpr)) {
        if (context->min_sublevels_up == 0) {
            elog(ERROR, "cannot push down CurrentOfExpr");
        }
        return false;
    }

    // Handle aggregate functions
    if (IsA(node, Aggref)) {
        Aggref *agg = (Aggref *) node;
        if (agg->agglevelsup >= context->min_sublevels_up) {
            agg->agglevelsup += context->delta_sublevels_up;
        }
        // Continue to recurse into arguments
    }

    // Handle grouping functions
    if (IsA(node, GroupingFunc)) {
        GroupingFunc *grp = (GroupingFunc *) node;
        if (grp->agglevelsup >= context->min_sublevels_up) {
            grp->agglevelsup += context->delta_sublevels_up;
        }
        // Continue to recurse into arguments
    }

    // Handle placeholder variables
    if (IsA(node, PlaceHolderVar)) {
        PlaceHolderVar *phv = (PlaceHolderVar *) node;
        if (phv->phlevelsup >= context->min_sublevels_up) {
            phv->phlevelsup += context->delta_sublevels_up;
        }
        // Continue to recurse into arguments
    }

    // Handle range table entries (specifically CTEs)
    if (IsA(node, RangeTblEntry)) {
        RangeTblEntry *rte = (RangeTblEntry *) node;
        if (rte->rtekind == RTE_CTE) {
            if (rte->ctelevelsup >= context->min_sublevels_up) {
                rte->ctelevelsup += context->delta_sublevels_up;
            }
        }
        return false;
    }

    // Handle subqueries - adjust context for deeper nesting
    if (IsA(node, Query)) {
        context->min_sublevels_up++;
        bool result = query_tree_walker((Query *) node,
                                       IncrementVarSublevelsUp_walker,
                                       (void *) context,
                                       QTW_EXAMINE_RTES_BEFORE);
        context->min_sublevels_up--;
        return result;
    }

    // Default: continue traversing expression tree
    return expression_tree_walker(node, IncrementVarSublevelsUp_walker, (void *) context);
}
```