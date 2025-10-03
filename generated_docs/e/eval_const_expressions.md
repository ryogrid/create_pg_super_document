# eval_const_expressions

## Location
[src/backend/optimizer/util/clauses.c:2254-2268](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/clauses.c#L2254-L2268)

## Overview
This function reduces recognizably constant subexpressions in an expression tree and applies boolean optimizations like "x OR true" => "true", serving as the main entry point for constant expression evaluation in PostgreSQL's query optimizer.

## Definition

```c
Node *
eval_const_expressions(PlannerInfo *root, Node *node)
```
## Detailed Description
The  function is a critical optimization component in PostgreSQL's query planner that performs constant folding and expression simplification. It reduces constant subexpressions (e.g., "2 + 2" => "4") and applies logical optimizations for boolean expressions. The function respects immutability constraints - only functions marked as "immutable" in pg_proc are pre-evaluated to ensure consistent results.

Key behaviors include:
- Flattens nested AND/OR clauses into N-argument form as expected by the planner
- Expands function calls requiring default arguments and converts named-argument calls to positional notation
- Tracks eliminated functions in root->glob->invalItems to maintain plan dependencies
- Prevents evaluation of functions like nextval() that produce non-constant results even with constant inputs

## Parameters / Member Variables
- `*root`: PlannerInfo pointer containing planner context and bound parameters; can be NULL if no Param substitutions or inlined function tracking is needed
- `*node`: Node pointer to the expression tree to be optimized and simplified
## Dependencies
- Functions called/Symbols referenced:
  - [eval_const_expressions_context](eval_const_expressions_context.md)
  - [eval_const_expressions_mutator](eval_const_expressions_mutator.md)
- Called from (representative examples):  
  - [preprocess_expression](../p/preprocess_expression.md) (planner.c:1202)
  - [expression_planner](expression_planner.md) (planner.c:6666)
  - [get_relation_constraints](../g/get_relation_constraints.md) (plancat.c:1315)
  - [apply_child_basequals](../a/apply_child_basequals.md) (inherit.c:870)

## Notes and Other Information
- The function assumes the input tree has already been type-checked and contains only reasonable operators/functions
- Critical for query performance as it eliminates unnecessary computation at execution time
- The planner relies on this function to flatten nested boolean expressions into the expected N-argument form
- Function calls requiring special handling (default arguments, named parameters) are normalized for executor compatibility

## Simplified Source

```c
Node *
eval_const_expressions(PlannerInfo *root, Node *node)
{
    eval_const_expressions_context context;

    // Set up evaluation context
    if (root)
        context.boundParams = root->glob->boundParams;  // Use bound parameters
    else
        context.boundParams = NULL;                     // No parameter binding

    // Initialize context for safe expression evaluation
    context.root = root;           // For tracking function dependencies
    context.active_fns = NIL;      // No recursive simplification active
    context.case_val = NULL;       // No CASE expression being examined
    context.estimate = false;      // Only safe transformations

    // Perform recursive constant expression evaluation
    return eval_const_expressions_mutator(node, &context);
}
```