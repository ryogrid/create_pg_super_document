# contain_mutable_functions_after_planning

## Location
[src/backend/optimizer/util/clauses.c:490-537](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/clauses.c#L490-L537)

## Overview
A wrapper function that safely tests for mutable functions in expressions from outside the planner by first running the expression through planning phases.

## Definition
```c
bool contain_mutable_functions_after_planning(Expr *expr)
```

## Detailed Description
This function provides a safe interface for checking expression mutability from contexts outside the query planner. It addresses two critical issues that can affect mutability analysis:

1. **Function Default Arguments**: The planner inserts default arguments for functions, which can significantly impact volatility analysis. For example, a function with a `default now()` parameter becomes volatile even if the base function is immutable.

2. **Function Inlining**: Inline-able functions are expanded during planning, potentially revealing that they are less volatile than their declared volatility level. This is particularly important for polymorphic functions, which must be conservatively marked with the most volatile behavior across all possible input types, but may be more stable for specific input types after inlining.

The function first applies `expression_planner()` to normalize the expression, then delegates to `contain_mutable_functions()` for the actual mutability check.

## Parameters / Member Variables
- `expr`: The expression to test for mutable functions, which will be processed through the planner first

## Dependencies
- Functions called/Symbols referenced:
  - [expression_planner](../e/expression_planner.md)
  - [contain_mutable_functions](contain_mutable_functions.md)
- Called from (representative examples):
  - [cookDefault](cookDefault.md) (catalog/heap.c)
  - [CheckPredicate](../C/CheckPredicate.md) (commands/indexcmds.c)
  - [ComputeIndexAttrs](../C/ComputeIndexAttrs.md) (commands/indexcmds.c)

## Notes and Other Information
- Designed specifically for use outside the planner context where expressions may not have been fully processed
- The function assumes that `expression_planner()` will not modify its input (read-only processing)
- Critical for index creation and constraint validation where accurate volatility assessment is essential
- Part of the public interface (non-static function) for mutability testing across PostgreSQL subsystems

## Simplified Source

```c
bool
contain_mutable_functions_after_planning(Expr *expr)
{
    // Run expression through planner to handle:
    // 1. Insert function default arguments (e.g., "default now()")
    // 2. Inline functions to reveal true volatility
    expr = expression_planner(expr);

    // Now check for mutable functions in the planned expression
    return contain_mutable_functions((Node *) expr);
}
```