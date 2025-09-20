# contain_mutable_functions

## Location
[src/backend/optimizer/util/clauses.c:370-375](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/clauses.c#L370-L375)

## Overview
Recursively searches for mutable functions within a clause, returning true if any mutable function or operator implemented by a mutable function is found.

## Definition

```c
bool
contain_mutable_functions(Node *clause)
```
## Detailed Description
This function serves as a wrapper around  to detect the presence of mutable functions within an expression tree. Mutable functions are those whose results can change between calls even with the same input parameters (like , , , etc.).

The primary purpose of this function is to prevent the query optimizer from incorrectly treating expressions containing mutable functions as constants. For example, a WHERE clause like "WHERE random() < 0.5" should not be considered a constant qualification because  returns different values on each call.

The function performs recursive traversal through the expression tree and will look into Query nodes (SubLink sub-selects) but deliberately avoids examining SubPlans for the same reasons as .

This function is designed to work on clauses that have been processed through expression preprocessing. For use cases outside the planner,  should be used instead.

## Parameters / Member Variables
- : The expression node to search for mutable function references

## Dependencies
- Functions called/Symbols referenced:
  - [contain_mutable_functions_walker](contain_mutable_functions_walker.md)
- Called from (representative examples):
  - [ComputePartitionAttrs](../C/ComputePartitionAttrs.md)
  - [check_index_predicates](check_index_predicates.md)
  - [create_indexscan_plan](create_indexscan_plan.md)
  - [create_bitmap_scan_plan](create_bitmap_scan_plan.md)
  - [can_minmax_aggs](can_minmax_aggs.md)
  - [contain_mutable_functions_after_planning](contain_mutable_functions_after_planning.md)
  - [eval_const_expressions_mutator](../e/eval_const_expressions_mutator.md)
  - [inline_function](../i/inline_function.md)
  - [relation_excluded_by_constraints](../r/relation_excluded_by_constraints.md)

## Notes and Other Information
- This is a wrapper function that delegates actual work to 
- Designed for use within the query planner on preprocessed expressions
- Prevents incorrect constant folding of expressions containing mutable functions
- Recursively examines Query nodes but not SubPlans
- For use outside the planner, prefer 
- Part of PostgreSQL's expression analysis and optimization safety checks