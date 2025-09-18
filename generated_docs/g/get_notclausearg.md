# get_notclausearg

## Location
src/include/nodes/nodeFuncs.h: 134 - 152

## Overview
Extracts the argument from a clause that is known to be a NOT clause (BoolExpr with NOT_EXPR operator).

## Definition
```c
static inline Expr *get_notclausearg(const void *notclause)
```

## Detailed Description
This function extracts the single argument from a NOT clause. It assumes the input is already validated to be a NOT clause (typically using is_notclause) and directly accesses the first (and only) argument from the BoolExpr's args list. Since NOT is a unary operator, it has exactly one argument that represents the expression being negated. This function provides a convenient way to access that negated expression for further processing.

## Parameters / Member Variables
- `notclause`: A pointer to a BoolExpr that is known to be a NOT clause; the caller is responsible for ensuring this precondition

## Dependencies
- Functions called/Symbols referenced:
  - BoolExpr (structure type)
  - linitial (macro to get the first element of a list)
  - Expr (return type)
- Called from (representative examples):
  - clause_selectivity_ext
  - match_boolean_index_clause
  - predicate_implied_by_simple_clause
  - dependency_is_compatible_clause
  - match_boolean_partition_clause

## Notes and Other Information
- This is an inline function defined in a header file for performance
- Assumes the input is a valid NOT clause - no validation is performed within this function
- Should typically be used in conjunction with is_notclause for safety
- The function directly accesses the first element of the args list since NOT expressions have exactly one argument
- Used throughout the optimizer when processing negated expressions for selectivity estimation, index matching, and constraint checking