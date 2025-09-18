# is_pseudo_constant_clause_relids

## Location
src/backend/optimizer/util/clauses.c: 2108 - 2129

## Overview
An optimized version of is_pseudo_constant_clause() that accepts pre-computed relation membership information to avoid redundant variable scanning.

## Definition
```c
bool is_pseudo_constant_clause_relids(Node *clause, Relids relids)
```

## Detailed Description
This function performs the same pseudo-constancy check as is_pseudo_constant_clause() but with a performance optimization. Instead of scanning the expression tree to find variables, it accepts a pre-computed Relids bitmap that indicates which relations the expression references.

The function determines if an expression is pseudo constant by:
1. Checking if the relids bitmap is empty (no relation references)
2. Verifying the expression contains no volatile functions

This optimization is valuable when the caller has already computed the relation membership information during other analysis phases, eliminating the need for a redundant contain_var_clause() scan.

## Parameters / Member Variables
- `clause`: The expression node to evaluate for pseudo-constancy
- `relids`: Pre-computed bitmap indicating which relations are referenced by the expression

## Dependencies
- Functions called/Symbols referenced:
  - bms_is_empty (checks if the relation bitmap is empty)
  - contain_volatile_functions (checks for volatile functions in the expression)
- Called from (representative examples):
  - clauselist_selectivity_ext (in selectivity estimation)

## Notes and Other Information
- Performance optimization over is_pseudo_constant_clause() when relation membership is already known
- Assumes the caller has accurately computed the relids parameter
- Like its counterpart, does not check for aggregates or window functions
- Primarily used in query optimization contexts where relation analysis has already been performed