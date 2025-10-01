# test_opexpr_is_hashable

## Location
[src/backend/optimizer/plan/subselect.c:792-831](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/subselect.c#L792-L831)

## Overview
Tests whether an OpExpr can be used for hash-based subplan execution by checking if the operator is hashable and the operands are properly structured for outer/inner query relationships.

## Definition

```c
structure, so we have to check.
	 * Such cases do not occur often enough to be worth trying to optimize, so
	 * we don't worry about trying to commute the clause or anything like
	 * that;
```
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
  - [list_length](../l/list_length.md)
- Called from (representative examples):
  - [testexpr_is_hashable](testexpr_is_hashable.md)

## Notes and Other Information
- The function assumes stricter conditions than plain operator strictness - the operator cannot yield NULL for non-null inputs
- This requirement aligns with assumptions made by hash indexes and hash joins
- The validation prevents invalid plan generation when function inlining has modified expression structure
- Performance optimization is not a concern due to the infrequency of problematic cases

## Simplified Source

```c
static bool test_opexpr_is_hashable(OpExpr *testexpr, List *param_ids) {
    // Check if operator is hashable and strict (required for hash operations)
    if (!hash_ok_operator(testexpr))
        return false;

    // Must have exactly 2 arguments (left and right operands)
    if (list_length(testexpr->args) != 2)
        return false;

    // Left side must not contain subquery parameters
    if (contain_exec_param((Node *) linitial(testexpr->args), param_ids))
        return false;

    // Right side must not contain outer query variables
    if (contain_var_clause((Node *) lsecond(testexpr->args)))
        return false;

    return true;
}
```