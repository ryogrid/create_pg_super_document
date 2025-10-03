# estimate_expression_value

## Location
[src/backend/optimizer/util/clauses.c:2395-2416](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/clauses.c#L2395-L2416)

## Overview
This function provides a more aggressive version of constant expression evaluation specifically for planning purposes, performing optimizations that are reasonable for estimation but may not be 100% safe for execution.

## Definition

```c
Node *
estimate_expression_value(PlannerInfo *root, Node *node)
```
## Detailed Description
The  function serves as an enhanced expression evaluator designed specifically for query planning and cost estimation scenarios. Unlike the standard , this function performs additional optimizations that prioritize useful estimates over absolute safety, making it ideal for planner decision-making where approximate values are acceptable.

The function extends standard constant folding with three additional optimization steps:
1. **Parameter substitution**: Uses bound parameter values even when parameters aren't marked as constant, effectively planning with the first supplied parameter value
2. **Stable function folding**: Evaluates stable functions (in addition to immutable ones) as constants for estimation purposes  
3. **PlaceHolderVar reduction**: Simplifies PlaceHolderVar nodes to their underlying expressions

This more permissive approach enables better cost estimates and planning decisions while maintaining the safety boundary between planning and execution phases.

## Parameters / Member Variables
- `*root`: PlannerInfo pointer containing planner context and bound parameters needed for parameter substitution and estimation
- `*node`: Node pointer to the expression tree to be evaluated and optimized for planning estimation
## Dependencies
- Functions called/Symbols referenced:
  - [eval_const_expressions_context](eval_const_expressions_context.md)  
  - [eval_const_expressions_mutator](eval_const_expressions_mutator.md)
- Called from (representative examples):
  - [clause_selectivity_ext](../c/clause_selectivity_ext.md) (clausesel.c:783)
  - [preprocess_limit](../p/preprocess_limit.md) (planner.c:2489, 2512)
  - [scalararraysel](../s/scalararraysel.md) (selfuncs.c:1844, 1845)
  - [get_restriction_variable](../g/get_restriction_variable.md) (selfuncs.c:4924, 4932)

## Notes and Other Information
- Sets context.estimate = true to enable unsafe transformations not allowed in standard evaluation
- Sets context.root = NULL since plan dependency tracking is not needed for estimation
- Critical for selectivity estimation, cost calculation, and limit preprocessing in the query planner
- Used extensively in statistics and cost estimation functions throughout the optimizer
- The "unsafe" transformations are acceptable because results are only used for planning, not execution

## Simplified Source

```c
Node *
estimate_expression_value(PlannerInfo *root, Node *node)
{
    eval_const_expressions_context context;

    // Set up context for aggressive estimation mode
    context.boundParams = root->glob->boundParams;  // Use bound parameters
    context.root = NULL;                            // No plan dependency tracking
    context.active_fns = NIL;                       // No recursive simplification tracking
    context.case_val = NULL;                        // No CASE context
    context.estimate = true;                        // Enable unsafe transformations

    // Apply enhanced constant folding with estimation-specific optimizations
    return eval_const_expressions_mutator(node, &context);
}
```