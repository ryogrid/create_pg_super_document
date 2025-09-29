# ReplaceVarsFromTargetList

## Location
[src/backend/rewrite/rewriteManip.c:1774-1793](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/rewrite/rewriteManip.c#L1774-L1793)

## Overview
This function replaces Var nodes in an expression tree by matching them against entries in a target list, primarily used during query rewriting operations.

## Definition
```c
Node *ReplaceVarsFromTargetList(Node *node,
                              int target_varno, int sublevels_up,
                              RangeTblEntry *target_rte,
                              List *targetlist,
                              ReplaceVarsNoMatchOption nomatch_option,
                              int nomatch_varno,
                              bool *outer_hasSubLinks)
```

## Detailed Description
`ReplaceVarsFromTargetList` is a specialized variable replacement function used in PostgreSQL's query rewriting system. It traverses an expression tree and replaces Var nodes that match a specific relation (identified by `target_varno`) with corresponding expressions from a target list. This function is commonly used when rewriting rules or view definitions need to substitute variable references with actual expressions.

The function uses a callback-based approach through `replace_rte_variables`, which handles the tree traversal while `ReplaceVarsFromTargetList_callback` performs the actual replacement logic. When a Var node is found that matches the target relation, it looks up the corresponding entry in the provided target list and substitutes it.

## Parameters / Member Variables
- `node`: The expression tree (Node) to process for variable replacement
- `target_varno`: The range table entry number identifying which relation's variables to replace
- `sublevels_up`: The nesting level for handling subqueries (0 for current level)
- `target_rte`: Pointer to the RangeTblEntry for the target relation
- `targetlist`: List of TargetEntry nodes containing replacement expressions
- `nomatch_option`: Specifies behavior when no matching target list entry is found (REPLACEVARS_REPORT_ERROR, REPLACEVARS_CHANGE_VARNO, or REPLACEVARS_SUBSTITUTE_NULL)
- `nomatch_varno`: Alternative varno to use when nomatch_option is REPLACEVARS_CHANGE_VARNO
- `outer_hasSubLinks`: Output parameter indicating whether SubLink nodes were encountered during processing

## Dependencies
- Functions called/Symbols referenced:
  - [replace_rte_variables](../r/replace_rte_variables.md)
  - [ReplaceVarsFromTargetList_callback](ReplaceVarsFromTargetList_callback.md)
  - `ReplaceVarsFromTargetList_context` (struct)
  - [ReplaceVarsNoMatchOption](ReplaceVarsNoMatchOption.md) (enum)

- Called from (representative examples):
  - [subquery_push_qual](../s/subquery_push_qual.md) (src/backend/optimizer/path/allpaths.c:3975)
  - [rewriteRuleAction](../r/rewriteRuleAction.md) (src/backend/rewrite/rewriteHandler.c:639, 671)
  - [CopyAndAddInvertedQual](../C/CopyAndAddInvertedQual.md) (src/backend/rewrite/rewriteHandler.c:2334)
  - [rewriteTargetView](../r/rewriteTargetView.md) (src/backend/rewrite/rewriteHandler.c:3559, 3710)

## Notes and Other Information
- Located in src/backend/rewrite/rewriteManip.c:1774-1793
- Uses a context structure (`ReplaceVarsFromTargetList_context`) to pass parameters to the callback function
- Handles whole-tuple references by expanding them into RowExpr constructs
- Part of PostgreSQL's rule system and view rewriting infrastructure
- The function is designed to be flexible in handling cases where target list entries don't match variables through the `nomatch_option` parameter

## Simplified Source

```c
Node *
ReplaceVarsFromTargetList(Node *node,
                         int target_varno, int sublevels_up,
                         RangeTblEntry *target_rte,
                         List *targetlist,
                         ReplaceVarsNoMatchOption nomatch_option,
                         int nomatch_varno,
                         bool *outer_hasSubLinks)
{
    ReplaceVarsFromTargetList_context context;

    // Set up context for callback function
    context.target_rte = target_rte;
    context.targetlist = targetlist;
    context.nomatch_option = nomatch_option;
    context.nomatch_varno = nomatch_varno;

    // Delegate to generic variable replacement function
    return replace_rte_variables(node, target_varno, sublevels_up,
                                ReplaceVarsFromTargetList_callback,
                                (void *) &context,
                                outer_hasSubLinks);
}
```