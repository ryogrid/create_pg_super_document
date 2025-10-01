# replace_vars_in_jointree

## Location
[src/backend/optimizer/prep/prepjointree.c:2368-2473](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/prep/prepjointree.c#L2368-L2473)

## Overview
Helper routine for perform_pullup_replace_vars that recursively processes every expression in the jointree structure, performing variable replacement without modifying the jointree structure itself.

## Definition
```c
static void replace_vars_in_jointree(Node *jtnode,
                                    pullup_replace_vars_context *context)
```

## Detailed Description
This function recursively traverses the jointree structure and performs variable replacement on all expressions contained within it, while preserving the tree structure itself. It handles different types of jointree nodes (RangeTblRef, FromExpr, JoinExpr) and applies appropriate variable replacement logic to each.

The function has special handling for:
- **LATERAL subqueries**: Processes expressions in LATERAL subqueries that might reference the target subquery being pulled up
- **Different RTE types**: Handles various range table entry types (relations with tablesample, subqueries, functions, table functions, values lists)
- **Join expressions**: Applies special PHV wrapping rules for full outer joins to maintain proper expression attribution
- **Recursive processing**: Calls itself recursively to process nested jointree structures

The function is careful to use PlaceHolderVars (PHVs) appropriately, particularly forcing their use in full outer join quals to prevent planning failures.

## Parameters / Member Variables
- `jtnode`: The jointree node to process (can be RangeTblRef, FromExpr, JoinExpr, or NULL)
- `context`: Context structure containing substitution mappings and control flags for the replacement operation

## Dependencies
- Functions called/Symbols referenced:
  - [pullup_replace_vars](../p/pullup_replace_vars.md)
  - [pullup_replace_vars_subquery](../p/pullup_replace_vars_subquery.md)
  - rt_fetch
  - nodeTag
  - [RangeTblRef](../R/RangeTblRef.md), FromExpr, JoinExpr
  - Various RTE types (RTE_RELATION, RTE_SUBQUERY, RTE_FUNCTION, etc.)
  - [TableSampleClause](../T/TableSampleClause.md), TableFunc
  - JOIN_FULL
- Called from (representative examples):
  - [perform_pullup_replace_vars](../p/perform_pullup_replace_vars.md)
  - [replace_vars_in_jointree](replace_vars_in_jointree.md) (recursive calls)

## Notes and Other Information
- Recursively calls itself to process nested jointree structures (FromExpr fromlist and JoinExpr left/right args)
- Forces PHV wrapping for expressions in full outer join quals to prevent planning issues
- Only processes LATERAL RTEs that are not the target subquery itself
- Includes assertions to validate that certain RTE types should not be marked LATERAL
- Handles the ugly necessity of processing expressions without changing the underlying jointree structure

## Simplified Source

```c
static void replace_vars_in_jointree(Node *jtnode, pullup_replace_vars_context *context)
{
    if (jtnode == NULL)
        return;

    if (IsA(jtnode, RangeTblRef)) {
        // Handle LATERAL subqueries that might reference target subquery
        int varno = ((RangeTblRef *) jtnode)->rtindex;

        if (varno != context->varno) {  // Skip target subquery itself
            RangeTblEntry *rte = rt_fetch(varno, context->root->parse->rtable);

            if (rte->lateral) {
                // Process different RTE types that can be LATERAL
                switch (rte->rtekind) {
                    case RTE_RELATION:
                        rte->tablesample = (TableSampleClause *)
                            pullup_replace_vars((Node *) rte->tablesample, context);
                        break;
                    case RTE_SUBQUERY:
                        rte->subquery = pullup_replace_vars_subquery(rte->subquery, context);
                        break;
                    case RTE_FUNCTION:
                        rte->functions = (List *)
                            pullup_replace_vars((Node *) rte->functions, context);
                        break;
                    case RTE_TABLEFUNC:
                        rte->tablefunc = (TableFunc *)
                            pullup_replace_vars((Node *) rte->tablefunc, context);
                        break;
                    case RTE_VALUES:
                        rte->values_lists = (List *)
                            pullup_replace_vars((Node *) rte->values_lists, context);
                        break;
                    // Other RTE types shouldn't be LATERAL
                }
            }
        }
    }
    else if (IsA(jtnode, FromExpr)) {
        // Process fromlist and WHERE clause
        FromExpr *f = (FromExpr *) jtnode;

        foreach(l, f->fromlist)
            replace_vars_in_jointree(lfirst(l), context);
        f->quals = pullup_replace_vars(f->quals, context);
    }
    else if (IsA(jtnode, JoinExpr)) {
        // Process join arguments and quals
        JoinExpr *j = (JoinExpr *) jtnode;
        bool save_wrap_non_vars = context->wrap_non_vars;

        replace_vars_in_jointree(j->larg, context);
        replace_vars_in_jointree(j->rarg, context);

        // Force PHV wrapping for full joins to prevent planning issues
        if (j->jointype == JOIN_FULL)
            context->wrap_non_vars = true;

        j->quals = pullup_replace_vars(j->quals, context);
        context->wrap_non_vars = save_wrap_non_vars;
    }
}
```