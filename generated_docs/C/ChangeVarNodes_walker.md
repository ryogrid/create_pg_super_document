# ChangeVarNodes_walker

## Location
[src/backend/rewrite/rewriteManip.c:565-674](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/rewrite/rewriteManip.c#L565-L674)

## Overview
A static walker function that recursively traverses expression trees to update range table references, changing all references from an old range table index to a new one at a specific sublevel.

## Definition
```c
static bool ChangeVarNodes_walker(Node *node, ChangeVarNodes_context *context)
```

## Detailed Description
This function is the core worker function for the ChangeVarNodes operation. It implements a tree-walking algorithm that visits each node in an expression tree and updates various types of range table references from an old index to a new index. The function handles multiple PostgreSQL node types including Var nodes, CurrentOfExpr, RangeTblRef, JoinExpr, PlaceHolderVar, PlanRowMark, and AppendRelInfo nodes.

The function operates at a specific sublevel context, allowing it to correctly handle nested subqueries and correlated references. It uses the expression_tree_walker and query_tree_walker infrastructure to ensure complete traversal of the tree structure.

## Parameters / Member Variables
- `node`: The current node being processed in the tree traversal
- `context`: A ChangeVarNodes_context structure containing:
  - `rt_index`: The original range table index to be replaced
  - `new_index`: The new range table index to replace the old one
  - `sublevels_up`: The sublevel at which to perform the replacement

## Dependencies
- Functions called/Symbols referenced:
  - [adjust_relid_set](../a/adjust_relid_set.md)
  - query_tree_walker
  - expression_tree_walker
  - IsA (macro for node type checking)
- Called from (representative examples):
  - [ChangeVarNodes](ChangeVarNodes.md) (recursive self-call through walker infrastructure)
  - query_tree_walker (for subquery traversal)
  - expression_tree_walker (for expression traversal)

## Notes and Other Information
- This is a static function used only within rewriteManip.c
- The function handles sublevel tracking to ensure correct operation in nested subqueries
- It includes assertions to verify that certain planner auxiliary nodes (SpecialJoinInfo, PlaceHolderInfo, MinMaxAggInfo) are not encountered at this stage
- For Query nodes, it increments the sublevel counter before recursing and decrements it afterward
- The function updates not just the primary range table references but also related nulling relations in Var and PlaceHolderVar nodes
- Returns false to continue tree traversal in most cases

## Simplified Source

```c
static bool
ChangeVarNodes_walker(Node *node, ChangeVarNodes_context *context)
{
    if (node == NULL)
        return false;

    // Handle Var nodes - update range table references and nulling relations
    if (IsA(node, Var)) {
        Var *var = (Var *) node;

        if (var->varlevelsup == context->sublevels_up) {
            if (var->varno == context->rt_index)
                var->varno = context->new_index;

            var->varnullingrels = adjust_relid_set(var->varnullingrels,
                                                  context->rt_index,
                                                  context->new_index);

            if (var->varnosyn == context->rt_index)
                var->varnosyn = context->new_index;
        }
        return false;
    }

    // Handle CurrentOfExpr nodes
    if (IsA(node, CurrentOfExpr)) {
        CurrentOfExpr *cexpr = (CurrentOfExpr *) node;
        if (context->sublevels_up == 0 && cexpr->cvarno == context->rt_index)
            cexpr->cvarno = context->new_index;
        return false;
    }

    // Handle RangeTblRef nodes
    if (IsA(node, RangeTblRef)) {
        RangeTblRef *rtr = (RangeTblRef *) node;
        if (context->sublevels_up == 0 && rtr->rtindex == context->rt_index)
            rtr->rtindex = context->new_index;
        return false;
    }

    // Handle JoinExpr nodes
    if (IsA(node, JoinExpr)) {
        JoinExpr *j = (JoinExpr *) node;
        if (context->sublevels_up == 0 && j->rtindex == context->rt_index)
            j->rtindex = context->new_index;
        // Continue to examine children
    }

    // Handle PlaceHolderVar nodes
    if (IsA(node, PlaceHolderVar)) {
        PlaceHolderVar *phv = (PlaceHolderVar *) node;
        if (phv->phlevelsup == context->sublevels_up) {
            phv->phrels = adjust_relid_set(phv->phrels,
                                          context->rt_index,
                                          context->new_index);
            phv->phnullingrels = adjust_relid_set(phv->phnullingrels,
                                                 context->rt_index,
                                                 context->new_index);
        }
        // Continue to examine children
    }

    // Handle PlanRowMark nodes
    if (IsA(node, PlanRowMark)) {
        PlanRowMark *rowmark = (PlanRowMark *) node;
        if (context->sublevels_up == 0) {
            if (rowmark->rti == context->rt_index)
                rowmark->rti = context->new_index;
            if (rowmark->prti == context->rt_index)
                rowmark->prti = context->new_index;
        }
        return false;
    }

    // Handle AppendRelInfo nodes
    if (IsA(node, AppendRelInfo)) {
        AppendRelInfo *appinfo = (AppendRelInfo *) node;
        if (context->sublevels_up == 0) {
            if (appinfo->parent_relid == context->rt_index)
                appinfo->parent_relid = context->new_index;
            if (appinfo->child_relid == context->rt_index)
                appinfo->child_relid = context->new_index;
        }
        // Continue to examine children
    }

    // Handle subqueries with sublevel tracking
    if (IsA(node, Query)) {
        context->sublevels_up++;
        bool result = query_tree_walker((Query *) node, ChangeVarNodes_walker,
                                       (void *) context, 0);
        context->sublevels_up--;
        return result;
    }

    // Continue recursive traversal for other node types
    return expression_tree_walker(node, ChangeVarNodes_walker,
                                 (void *) context);
}
```