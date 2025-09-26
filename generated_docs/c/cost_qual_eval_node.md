# cost_qual_eval_node

## Location
[src/backend/optimizer/path/costsize.c:4669-4682](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/costsize.c#L4669-L4682)

## Overview
Estimates the CPU costs of evaluating a single qualification expression or RestrictInfo node.

## Definition

```c
void
cost_qual_eval_node(QualCost *cost, Node *qual, PlannerInfo *root)
```
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
  - [cost_qual_eval_walker](cost_qual_eval_walker.md)
  - cost_qual_eval_context (struct)
  - [QualCost](../Q/QualCost.md) (struct)
- Called from (representative examples):
  - [cost_functionscan](cost_functionscan.md)
  - [cost_tablefuncscan](cost_tablefuncscan.md)
  - [cost_windowagg](cost_windowagg.md)
  - [cost_qual_eval_walker](cost_qual_eval_walker.md)
  - [set_rel_width](../s/set_rel_width.md)
  - [set_pathtarget_cost_width](../s/set_pathtarget_cost_width.md)
  - [get_agg_clause_costs](../g/get_agg_clause_costs.md)

## Notes and Other Information
- Functionally equivalent to  but operates on single expressions instead of lists
- Widely used throughout the planner for individual expression cost evaluation
- Commonly used in contexts involving expression tree traversal and analysis
- Root parameter can be NULL, which may reduce estimation accuracy but allows broader usage
- Essential for cost calculations in aggregate functions, window functions, and target list evaluations
- More efficient than  when only single expressions need evaluation