# estimate_expression_value

## Location
src/backend/optimizer/util/clauses.c: 2395 - 2416

## Overview
This function provides a more aggressive version of constant expression evaluation specifically for planning purposes, performing optimizations that are reasonable for estimation but may not be 100% safe for execution.

## Definition


## Detailed Description
The  function serves as an enhanced expression evaluator designed specifically for query planning and cost estimation scenarios. Unlike the standard , this function performs additional optimizations that prioritize useful estimates over absolute safety, making it ideal for planner decision-making where approximate values are acceptable.

The function extends standard constant folding with three additional optimization steps:
1. **Parameter substitution**: Uses bound parameter values even when parameters aren't marked as constant, effectively planning with the first supplied parameter value
2. **Stable function folding**: Evaluates stable functions (in addition to immutable ones) as constants for estimation purposes  
3. **PlaceHolderVar reduction**: Simplifies PlaceHolderVar nodes to their underlying expressions

This more permissive approach enables better cost estimates and planning decisions while maintaining the safety boundary between planning and execution phases.

## Parameters / Member Variables
- : PlannerInfo pointer containing planner context and bound parameters needed for parameter substitution and estimation
- : Node pointer to the expression tree to be evaluated and optimized for planning estimation

## Dependencies
- Functions called/Symbols referenced:
  - eval_const_expressions_context  
  - eval_const_expressions_mutator
- Called from (representative examples):
  - clause_selectivity_ext (clausesel.c:783)
  - preprocess_limit (planner.c:2489, 2512)
  - scalararraysel (selfuncs.c:1844, 1845)
  - get_restriction_variable (selfuncs.c:4924, 4932)

## Notes and Other Information
- Sets context.estimate = true to enable unsafe transformations not allowed in standard evaluation
- Sets context.root = NULL since plan dependency tracking is not needed for estimation
- Critical for selectivity estimation, cost calculation, and limit preprocessing in the query planner
- Used extensively in statistics and cost estimation functions throughout the optimizer
- The "unsafe" transformations are acceptable because results are only used for planning, not execution