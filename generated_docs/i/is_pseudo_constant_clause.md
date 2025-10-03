# is_pseudo_constant_clause

## Location
[src/backend/optimizer/util/clauses.c:2088-2107](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/clauses.c#L2088-L2107)

## Overview
Determines whether an expression is "pseudo constant" for query optimization, meaning it contains no variables from the current query level and no volatile functions.

## Definition

```c
bool
is_pseudo_constant_clause(Node *clause)
```
## Detailed Description
This function detects whether an expression can be considered "pseudo constant" during query optimization. A pseudo constant expression is one that:
- Contains no variables (Vars) of the current query level
- Contains no volatile functions

While not necessarily a true constant (it can contain parameters and outer-level variables), a pseudo constant expression's value remains constant throughout any single scan of the current query. This property makes it suitable for use in optimization scenarios such as index scan keys.

The function uses a two-stage check strategy for performance: it first checks for variables (which is faster) and only checks for volatile functions if no variables are found. This optimization is based on the assumption that the volatile function check is more expensive and less likely to fail.

Important limitation: This function does not check for aggregates (Aggrefs) or window functions (WindowFuncs), as it's primarily designed for WHERE clause analysis where such constructs are not expected.

## Parameters / Member Variables
- `*clause`: The expression node to evaluate for pseudo-constancy
## Dependencies
- Functions called/Symbols referenced:
  - [contain_var_clause](../c/contain_var_clause.md) (checks for variables in the expression)
  - [contain_volatile_functions](../c/contain_volatile_functions.md) (checks for volatile functions in the expression)
- Called from (representative examples):
  - [find_window_run_conditions](../f/find_window_run_conditions.md)
  - [clauselist_selectivity_ext](../c/clauselist_selectivity_ext.md)
  - [dependency_is_compatible_clause](../d/dependency_is_compatible_clause.md)
  - [dependency_is_compatible_expression](../d/dependency_is_compatible_expression.md)

## Notes and Other Information
- Designed primarily for WHERE clause analysis in query optimization
- Does not detect aggregates or window functions - use contain_agg_clause() for complete pseudo-constness checking in other contexts
- Performance-optimized with a two-stage checking strategy
- Critical for determining whether expressions can be used as index scan keys or moved to different parts of the query plan

## Simplified Source

```c
bool
is_pseudo_constant_clause(Node *clause)
{
    // Two-stage check for performance optimization:
    // 1. Check for variables first (faster)
    // 2. Only check volatile functions if no variables found
    if (!contain_var_clause(clause) &&
        !contain_volatile_functions(clause))
        return true;
    return false;
}
```