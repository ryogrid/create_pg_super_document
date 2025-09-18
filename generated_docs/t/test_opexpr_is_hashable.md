# test_opexpr_is_hashable

## Location
src/backend/optimizer/plan/subselect.c: 792 - 831

## Overview
Tests whether an OpExpr can be used for hash-based subplan execution by checking if the operator is hashable and the operands are properly structured for outer/inner query relationships.

## Definition


## Detailed Description
This function validates whether an OpExpr (operator expression) can be safely used in hash-based subplan execution strategies. It performs several critical checks:

1. **Operator Validation**: Ensures the combining operator is both hashable and strict, which is essential for hash-based operations and predictable null handling.
2. **Structural Validation**: Verifies the expression has exactly two arguments.
3. **Parameter Placement**: Ensures that parameters supplied by the subquery do not appear in the left-hand side (LHS).
4. **Variable Placement**: Confirms that outer query variables do not appear in the right-hand side (RHS).

The function is designed to handle cases where function inlining might have altered the original parser-generated structure of ANY SubLink test expressions.

## Parameters
- : The OpExpr to be tested for hash compatibility
- : List of parameter IDs that will be supplied by the subquery

## Dependencies
- Functions called/Symbols referenced:
  - [hash_ok_operator](../h/hash_ok_operator.md)
  - [contain_exec_param](../c/contain_exec_param.md)
  - [contain_var_clause](../c/contain_var_clause.md)
  - linitial
  - lsecond
  - list_length
- Called from (representative examples):
  - [testexpr_is_hashable](testexpr_is_hashable.md)

## Notes and Other Information
- The function assumes stricter conditions than plain operator strictness - the operator cannot yield NULL for non-null inputs
- This requirement aligns with assumptions made by hash indexes and hash joins
- The validation prevents invalid plan generation when function inlining has modified expression structure
- Performance optimization is not a concern due to the infrequency of problematic cases