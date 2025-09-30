# fix_join_expr_mutator

## Location
[src/backend/optimizer/plan/setrefs.c:3055-3193](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/setrefs.c#L3055-L3193)

## Overview
A recursive expression tree mutator function that fixes variable references in expressions for join nodes by mapping them to references from the input target lists of outer and inner child plans.

## Definition

```c
static Node *
fix_join_expr_mutator(Node *node, fix_join_expr_context *context)
```
## Detailed Description
The  function is a critical component of PostgreSQL's query plan reference fixing process. It transforms expressions in join nodes by replacing variable references with appropriate references to the target lists of child plans. The function operates as a tree walker that recursively processes expression nodes and performs the following key operations:

1. **Variable Reference Resolution**: For  nodes, it searches the outer and inner input target lists to find matching variables and replaces them with appropriate references (OUTER_VAR or INNER_VAR).

2. **PlaceHolderVar Handling**: For  nodes, it attempts to find them in the input target lists, and if not found, recursively processes the contained expression.

3. **Complex Expression Matching**: It tries to match more complex expressions against non-variable entries in the target lists.

4. **Special Node Processing**: It handles special cases like  nodes and  nodes with appropriate specialized functions.

The function ensures that all variable references in join expressions correctly point to the outputs of the join's child plans, which is essential for proper query execution.

## Parameters / Member Variables
- : The expression node to be processed and potentially transformed
- : A structure containing context information including:
  - : Indexed target list from the outer child plan
  - : Indexed target list from the inner child plan
  - : Relation ID that can be adjusted with rtoffset
  - : Range table offset for adjusting relation numbers
  - : Nulling-resilient matching flag
  - : PlannerInfo structure for additional context

## Dependencies
- Functions called/Symbols referenced:
  - [search_indexed_tlist_for_var](../s/search_indexed_tlist_for_var.md)
  - [search_indexed_tlist_for_phv](../s/search_indexed_tlist_for_phv.md)
  - [search_indexed_tlist_for_non_var](../s/search_indexed_tlist_for_non_var.md)
  - [copyVar](../c/copyVar.md)
  - [fix_param_node](fix_param_node.md)
  - [fix_alternative_subplan](fix_alternative_subplan.md)
  - [fix_expr_common](fix_expr_common.md)
  - expression_tree_mutator
- Called from (representative examples):
  - fix_scan_list
  - [fix_join_expr](fix_join_expr.md)
  - [fix_join_expr_mutator](fix_join_expr_mutator.md) (recursive calls)

## Notes and Other Information
- This function is part of the setrefs.c module which handles setting up references between plan nodes
- It uses a context-driven approach to maintain state across recursive calls
- The function prioritizes searching the outer target list before the inner target list
- Error handling includes an elog(ERROR) when a variable cannot be found in subplan target lists
- The function is static, indicating it's only used within the setrefs.c compilation unit
- It integrates with PostgreSQL's expression tree mutator framework for efficient tree traversal

## Simplified Source

```c
static Node *fix_join_expr_mutator(Node *node, fix_join_expr_context *context) {
    Var *newvar;

    if (node == NULL)
        return NULL;

    // Handle Variable nodes
    if (IsA(node, Var)) {
        Var *var = (Var *) node;

        // Search outer input target list first
        if (context->outer_itlist) {
            newvar = search_indexed_tlist_for_var(var, context->outer_itlist,
                                                  OUTER_VAR, context->rtoffset, context->nrm_match);
            if (newvar)
                return (Node *) newvar;
        }

        // Then search inner input target list
        if (context->inner_itlist) {
            newvar = search_indexed_tlist_for_var(var, context->inner_itlist,
                                                  INNER_VAR, context->rtoffset, context->nrm_match);
            if (newvar)
                return (Node *) newvar;
        }

        // Handle acceptable_rel case
        if (var->varno == context->acceptable_rel) {
            var = copyVar(var);
            var->varno += context->rtoffset;
            if (var->varnosyn > 0)
                var->varnosyn += context->rtoffset;
            return (Node *) var;
        }

        elog(ERROR, "variable not found in subplan target lists");
    }

    // Handle PlaceHolderVar nodes
    if (IsA(node, PlaceHolderVar)) {
        PlaceHolderVar *phv = (PlaceHolderVar *) node;

        // Try to find in outer target list
        if (context->outer_itlist && context->outer_itlist->has_ph_vars) {
            newvar = search_indexed_tlist_for_phv(phv, context->outer_itlist,
                                                  OUTER_VAR, context->nrm_match);
            if (newvar)
                return (Node *) newvar;
        }

        // Try to find in inner target list
        if (context->inner_itlist && context->inner_itlist->has_ph_vars) {
            newvar = search_indexed_tlist_for_phv(phv, context->inner_itlist,
                                                  INNER_VAR, context->nrm_match);
            if (newvar)
                return (Node *) newvar;
        }

        // Process contained expression if not found
        return fix_join_expr_mutator((Node *) phv->phexpr, context);
    }

    // Try matching complex expressions in target lists
    if (context->outer_itlist && context->outer_itlist->has_non_vars) {
        newvar = search_indexed_tlist_for_non_var((Expr *) node,
                                                  context->outer_itlist, OUTER_VAR);
        if (newvar)
            return (Node *) newvar;
    }

    if (context->inner_itlist && context->inner_itlist->has_non_vars) {
        newvar = search_indexed_tlist_for_non_var((Expr *) node,
                                                  context->inner_itlist, INNER_VAR);
        if (newvar)
            return (Node *) newvar;
    }

    // Handle special node types
    if (IsA(node, Param))
        return fix_param_node(context->root, (Param *) node);

    if (IsA(node, AlternativeSubPlan))
        return fix_join_expr_mutator(fix_alternative_subplan(context->root,
                                                            (AlternativeSubPlan *) node,
                                                            context->num_exec),
                                    context);

    // Apply common expression fixes and recurse
    fix_expr_common(context->root, node);
    return expression_tree_mutator(node, fix_join_expr_mutator, (void *) context);
}
```