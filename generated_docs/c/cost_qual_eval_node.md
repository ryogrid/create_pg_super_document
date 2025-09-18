# cost_qual_eval_node

## Location
src/backend/optimizer/path/costsize.c: 4669 - 4682

## Overview
Estimates the CPU costs of evaluating a single qualification expression or RestrictInfo node.

## Definition


## Detailed Description
The  function is a specialized version of  that estimates evaluation costs for a single qualification expression rather than a list of expressions. It provides the same startup and per-tuple cost estimation but operates on individual nodes, making it suitable for fine-grained cost analysis of specific expressions.

The function initializes a cost evaluation context and directly calls  on the single qualification node. This makes it efficient for scenarios where only one expression needs cost estimation, avoiding the overhead of list processing required by .

This function is particularly useful in contexts where expressions are processed individually, such as during expression tree walking, specific clause cost evaluation, or when building cost estimates incrementally.

## Parameters / Member Variables
- : Output parameter receiving the calculated QualCost structure with startup and per_tuple components
- : Single qualification expression (Node* or RestrictInfo*) to evaluate
- : PlannerInfo context for planning information (can be NULL, resulting in slightly worse estimates)

## Dependencies
- Functions called/Symbols referenced:
  - cost_qual_eval_walker
  - cost_qual_eval_context (struct)
  - QualCost (struct)
- Called from (representative examples):
  - cost_functionscan
  - cost_tablefuncscan
  - cost_windowagg
  - cost_qual_eval_walker
  - set_rel_width
  - set_pathtarget_cost_width
  - get_agg_clause_costs

## Notes and Other Information
- Functionally equivalent to  but operates on single expressions instead of lists
- Widely used throughout the planner for individual expression cost evaluation
- Commonly used in contexts involving expression tree traversal and analysis
- Root parameter can be NULL, which may reduce estimation accuracy but allows broader usage
- Essential for cost calculations in aggregate functions, window functions, and target list evaluations
- More efficient than  when only single expressions need evaluation