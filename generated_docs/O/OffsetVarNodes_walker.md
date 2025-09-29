# OffsetVarNodes_walker

## Location
[src/backend/rewrite/rewriteManip.c:392-480](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/rewrite/rewriteManip.c#L392-L480)

## Overview
A static tree walker function that adjusts variable node numbers and relation identifiers by a specified offset, handling various node types in expression trees and query structures.

## Definition

```c
static bool
OffsetVarNodes_walker(Node *node, OffsetVarNodes_context *context)
```
## Detailed Description
This function implements a recursive tree walker that adjusts relation identifiers throughout expression trees and query structures. It handles multiple node types including Var nodes, CurrentOfExpr, RangeTblRef, JoinExpr, PlaceHolderVar, and AppendRelInfo. The function respects query nesting levels (sublevels_up) to ensure that only variables at the appropriate query level are modified. For Var nodes, it adjusts varno, varnullingrels, and varnosyn. The function also handles subqueries by recursively calling itself with adjusted context levels.

## Parameters / Member Variables
- `node`: The current Node being processed in the tree traversal
- `context`: OffsetVarNodes_context structure containing offset value and current sublevel information

## Dependencies
- Functions called/Symbols referenced:
  - IsA (type checking macro)
  - [offset_relid_set](../o/offset_relid_set.md) (for adjusting relation ID sets)
  - query_tree_walker (for Query node recursion)
  - expression_tree_walker (for general expression recursion)
  - Assert (debugging assertions)
- Called from (representative examples):
  - [OffsetVarNodes](OffsetVarNodes.md) (main entry point)
  - [OffsetVarNodes_walker](OffsetVarNodes_walker.md) (recursive self-calls)

## Notes and Other Information
- This is a static function used internally by the OffsetVarNodes system
- Handles query nesting by tracking sublevels_up in the context
- Includes assertions to ensure it doesn't encounter unexpected planner auxiliary nodes
- Processes different node types with specific logic for each:
  - [Var](../V/Var.md): adjusts varno, varnullingrels, and varnosyn
  - [CurrentOfExpr](../C/CurrentOfExpr.md): adjusts cvarno at top level only
  - [RangeTblRef](../R/RangeTblRef.md): adjusts rtindex at top level only  
  - [JoinExpr](../J/JoinExpr.md): adjusts rtindex if present
  - [PlaceHolderVar](../P/PlaceHolderVar.md): adjusts phrels and phnullingrels
  - [AppendRelInfo](../A/AppendRelInfo.md): adjusts parent_relid and child_relid
- Used during query rewriting when combining range tables or adjusting variable references
- Critical for maintaining correct variable-to-relation mappings during query transformation

## Simplified Source

```c
static bool OffsetVarNodes_walker(Node *node, OffsetVarNodes_context *context) {
    if (node == NULL)
        return false;

    // Handle Var nodes - adjust variable numbers at target level
    if (IsA(node, Var)) {
        Var *var = (Var *) node;

        if (var->varlevelsup == context->sublevels_up) {
            var->varno += context->offset;
            var->varnullingrels = offset_relid_set(var->varnullingrels, context->offset);
            if (var->varnosyn > 0)
                var->varnosyn += context->offset;
        }
        return false;
    }

    // Handle CurrentOfExpr - adjust at top level only
    if (IsA(node, CurrentOfExpr)) {
        CurrentOfExpr *cexpr = (CurrentOfExpr *) node;

        if (context->sublevels_up == 0)
            cexpr->cvarno += context->offset;
        return false;
    }

    // Handle RangeTblRef - adjust table references
    if (IsA(node, RangeTblRef)) {
        RangeTblRef *rtr = (RangeTblRef *) node;

        if (context->sublevels_up == 0)
            rtr->rtindex += context->offset;
        return false;
    }

    // Handle JoinExpr - adjust join table index if present
    if (IsA(node, JoinExpr)) {
        JoinExpr *j = (JoinExpr *) node;

        if (j->rtindex && context->sublevels_up == 0)
            j->rtindex += context->offset;
        // Continue to examine children
    }

    // Handle PlaceHolderVar - adjust relation sets
    if (IsA(node, PlaceHolderVar)) {
        PlaceHolderVar *phv = (PlaceHolderVar *) node;

        if (phv->phlevelsup == context->sublevels_up) {
            phv->phrels = offset_relid_set(phv->phrels, context->offset);
            phv->phnullingrels = offset_relid_set(phv->phnullingrels, context->offset);
        }
        // Continue to examine children
    }

    // Handle AppendRelInfo - adjust parent and child relation IDs
    if (IsA(node, AppendRelInfo)) {
        AppendRelInfo *appinfo = (AppendRelInfo *) node;

        if (context->sublevels_up == 0) {
            appinfo->parent_relid += context->offset;
            appinfo->child_relid += context->offset;
        }
        // Continue to examine children
    }

    // Handle subqueries by adjusting nesting level
    if (IsA(node, Query)) {
        bool result;

        context->sublevels_up++;
        result = query_tree_walker((Query *) node, OffsetVarNodes_walker, (void *) context, 0);
        context->sublevels_up--;
        return result;
    }

    // Recursively process all other nodes
    return expression_tree_walker(node, OffsetVarNodes_walker, (void *) context);
}
```