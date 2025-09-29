# flatten_join_alias_vars_mutator

## Location
[src/backend/optimizer/util/var.c:767-909](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/var.c#L767-L909)

## Overview
The core recursive function that performs the actual flattening of join alias variables by traversing and transforming expression trees.

## Definition
```c
static Node *flatten_join_alias_vars_mutator(Node *node, flatten_join_alias_vars_context *context)
```

## Detailed Description
This static function implements the tree-walking logic for flattening join alias variables. It uses the expression tree mutator pattern to recursively process all nodes in an expression tree. The function handles several key node types:

**Var Nodes**: The primary focus of the function. For Vars referencing JOIN relations:
- Regular attribute references are replaced with the corresponding expression from the JOIN's joinaliasvars list
- Whole-row references (varattno == InvalidAttrNumber) are expanded into RowExpr constructs containing all non-dropped columns
- Variable level adjustments are applied for nested subqueries
- Original Var location information is preserved when possible

**PlaceHolderVar Nodes**: These are processed recursively with special handling for relid set adjustments. The function updates phrels to substitute base relations for join relids while preserving phnullingrels.

**Query Nodes**: Represents subqueries that require recursive processing with incremented sublevels_up and careful SubLink tracking.

The function also maintains important state:
- Tracks whether SubLinks are being inserted to update Query.hasSubLinks
- Preserves varnullingrels information through add_nullingrels_if_needed calls
- Handles variable level adjustments for expressions from upper query levels

## Parameters / Member Variables
- `node`: The expression node to be processed
- `context`: Context structure containing:
  - `root`: PlannerInfo for the current query level
  - `query`: The Query being processed
  - `sublevels_up`: Current nesting level for variable references
  - `possible_sublink`: Whether join aliases might contain SubLinks
  - `inserted_sublink`: Whether we've inserted any SubLinks during processing

## Dependencies
- Functions called/Symbols referenced:
  - rt_fetch, RTE_JOIN, InvalidAttrNumber
  - copyObject, IncrementVarSublevelsUp
  - RowExpr, COERCE_IMPLICIT_CAST
  - [add_nullingrels_if_needed](../a/add_nullingrels_if_needed.md)
  - [checkExprHasSubLink](../c/checkExprHasSubLink.md)
  - expression_tree_mutator, query_tree_mutator
  - [alias_relid_set](../a/alias_relid_set.md)
- Called from (representative examples):
  - [flatten_join_alias_vars](flatten_join_alias_vars.md) (initial call)
  - Recursive calls to itself during tree traversal

## Notes and Other Information
- The function asserts that it should not encounter already-planned SubPlan nodes or planner auxiliary nodes
- Whole-row expansion creates RowExpr with COERCE_IMPLICIT_CAST format and explicit column names
- The QTW_IGNORE_JOINALIASES flag prevents infinite recursion when processing Query nodes
- [Variable](../V/Variable.md)-free expressions in add_nullingrels_if_needed require special evaluation placement logic

## Simplified Source

```c
static Node *
flatten_join_alias_vars_mutator(Node *node, flatten_join_alias_vars_context *context)
{
    if (node == NULL)
        return NULL;

    if (IsA(node, Var))
    {
        Var *var = (Var *) node;
        RangeTblEntry *rte;
        Node *newvar;

        // Only process Vars at target nesting level
        if (var->varlevelsup != context->sublevels_up)
            return node;

        // Must be a JOIN relation
        rte = rt_fetch(var->varno, context->query->rtable);
        if (rte->rtekind != RTE_JOIN)
            return node;

        if (var->varattno == InvalidAttrNumber)
        {
            // Expand whole-row reference into RowExpr
            RowExpr *rowexpr = makeNode(RowExpr);
            List *fields = NIL;
            List *colnames = NIL;
            ListCell *lv, *ln;

            // Build field list from join alias vars
            forboth(lv, rte->joinaliasvars, ln, rte->eref->colnames)
            {
                newvar = (Node *) lfirst(lv);
                if (newvar == NULL)  // Skip dropped columns
                    continue;

                newvar = copyObject(newvar);

                // Adjust variable levels if needed
                if (context->sublevels_up != 0)
                    IncrementVarSublevelsUp(newvar, context->sublevels_up, 0);

                // Preserve location and recurse
                if (IsA(newvar, Var))
                    ((Var *) newvar)->location = var->location;
                newvar = flatten_join_alias_vars_mutator(newvar, context);

                fields = lappend(fields, newvar);
                colnames = lappend(colnames, copyObject((Node *) lfirst(ln)));
            }

            // Create RowExpr with proper typing
            rowexpr->args = fields;
            rowexpr->row_typeid = var->vartype;
            rowexpr->row_format = COERCE_IMPLICIT_CAST;
            rowexpr->colnames = colnames;
            rowexpr->location = var->location;

            return add_nullingrels_if_needed(context->root, (Node *) rowexpr, var);
        }

        // Expand regular join alias reference
        Assert(var->varattno > 0);
        newvar = (Node *) list_nth(rte->joinaliasvars, var->varattno - 1);
        Assert(newvar != NULL);
        newvar = copyObject(newvar);

        // Adjust variable levels and preserve location
        if (context->sublevels_up != 0)
            IncrementVarSublevelsUp(newvar, context->sublevels_up, 0);
        if (IsA(newvar, Var))
            ((Var *) newvar)->location = var->location;

        // Recurse and track sublinks
        newvar = flatten_join_alias_vars_mutator(newvar, context);
        if (context->possible_sublink && !context->inserted_sublink)
            context->inserted_sublink = checkExprHasSubLink(newvar);

        return add_nullingrels_if_needed(context->root, newvar, var);
    }

    if (IsA(node, PlaceHolderVar))
    {
        // Handle PlaceHolderVar with relid set updates
        PlaceHolderVar *phv = (PlaceHolderVar *) expression_tree_mutator(node,
                                                                         flatten_join_alias_vars_mutator,
                                                                         (void *) context);
        if (phv->phlevelsup == context->sublevels_up)
            phv->phrels = alias_relid_set(context->query, phv->phrels);

        return (Node *) phv;
    }

    if (IsA(node, Query))
    {
        // Handle subqueries with level tracking
        Query *newnode;
        bool save_inserted_sublink;

        context->sublevels_up++;
        save_inserted_sublink = context->inserted_sublink;
        context->inserted_sublink = ((Query *) node)->hasSubLinks;

        newnode = query_tree_mutator((Query *) node,
                                     flatten_join_alias_vars_mutator,
                                     (void *) context,
                                     QTW_IGNORE_JOINALIASES);

        newnode->hasSubLinks |= context->inserted_sublink;
        context->inserted_sublink = save_inserted_sublink;
        context->sublevels_up--;

        return (Node *) newnode;
    }

    // Continue recursive mutation
    return expression_tree_mutator(node, flatten_join_alias_vars_mutator, (void *) context);
}
```