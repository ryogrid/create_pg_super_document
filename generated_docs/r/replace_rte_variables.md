# replace_rte_variables

## Location
[src/backend/rewrite/rewriteManip.c:1346-1392](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/rewrite/rewriteManip.c#L1346-L1392)

## Overview
Finds all Vars in an expression tree that reference a particular RTE (Range Table Entry) and replaces them with substitute expressions obtained from a caller-supplied callback function.

## Definition

```c
Node *
replace_rte_variables(Node *node, int target_varno, int sublevels_up,
					  replace_rte_variables_callback callback,
					  void *callback_arg,
					  bool *outer_hasSubLinks)
```
## Detailed Description
This function performs a tree walk through PostgreSQL expression trees to find all Var nodes that reference a specific range table entry (identified by target_varno at a specific sublevel) and replaces them using a user-provided callback function. It's a key utility in PostgreSQL's query rewriting system, particularly useful for operations like view expansion, subquery pullup, and other query transformations.

The function handles SubLink tracking carefully - when a replacement introduces new SubLinks into the expression tree, it updates the hasSubLinks flag appropriately. This is crucial for maintaining query metadata consistency during transformations.

The function uses the query_or_expression_tree_mutator infrastructure to recursively process both Query nodes and bare expression trees, with special handling to avoid incrementing sublevels_up when starting with a Query node.

## Parameters / Member Variables
- `*node`: The expression tree or Query to process for variable replacement
- `target_varno`: The range table entry number whose variables should be replaced
- `sublevels_up`: The sublevel depth at which to look for the target RTE
- `callback`: User-provided function that determines the replacement expression for each matching Var
- `*callback_arg`: Additional context data passed to the callback function
- `*outer_hasSubLinks`: Pointer to hasSubLinks flag of containing Query (NULL if not in a Query context)
## Dependencies
- Functions called/Symbols referenced:
  - query_or_expression_tree_mutator
  - [replace_rte_variables_mutator](replace_rte_variables_mutator.md)
  - [replace_rte_variables_context](replace_rte_variables_context.md)
  - replace_rte_variables_callback
- Called from (representative examples):
  - [pullup_replace_vars](../p/pullup_replace_vars.md)
  - [pullup_replace_vars_subquery](../p/pullup_replace_vars_subquery.md)
  - [ReplaceVarsFromTargetList](../R/ReplaceVarsFromTargetList.md)

## Notes and Other Information
- The function exposes its mutator function and context struct publicly because callbacks often need to recurse directly to the mutator on sub-expressions
- [SubLink](../S/SubLink.md) handling is complex but necessary for maintaining query structure integrity during transformations
- Unlike aggregate and window function tracking, SubLink tracking requires special attention because replacements can introduce new subqueries
- The function can start with either a Query or bare expression tree, with appropriate handling for both cases
- Error handling ensures that if SubLinks are inserted but no place exists to record them, an error is raised

## Simplified Source

```c
Node *
replace_rte_variables(Node *node, int target_varno, int sublevels_up,
                      replace_rte_variables_callback callback,
                      void *callback_arg,
                      bool *outer_hasSubLinks)
{
    replace_rte_variables_context context;

    // Set up context for the tree walk
    context.callback = callback;
    context.callback_arg = callback_arg;
    context.target_varno = target_varno;
    context.sublevels_up = sublevels_up;

    // Initialize SubLink tracking based on current state
    if (node && IsA(node, Query))
        context.inserted_sublink = ((Query *) node)->hasSubLinks;
    else if (outer_hasSubLinks)
        context.inserted_sublink = *outer_hasSubLinks;
    else
        context.inserted_sublink = false;

    // Perform the tree walk and replacement
    Node *result = query_or_expression_tree_mutator(node,
                                                    replace_rte_variables_mutator,
                                                    (void *) &context,
                                                    0);

    // Update SubLink flags if new SubLinks were inserted
    if (context.inserted_sublink) {
        if (result && IsA(result, Query))
            ((Query *) result)->hasSubLinks = true;
        else if (outer_hasSubLinks)
            *outer_hasSubLinks = true;
        else
            elog(ERROR, "replace_rte_variables inserted a SubLink, but has no place to record it");
    }

    return result;
}
```