# eqsel_internal

## Location
src/backend/utils/adt/selfuncs.c: 237 - 295

## Overview
The eqsel_internal function is the core implementation for selectivity estimation of both equality (=) and inequality (<>) operators, providing the common logic shared between eqsel() and neqsel() functions.

## Definition


## Detailed Description
The eqsel_internal function performs the actual selectivity estimation calculations for equality and inequality operations. It analyzes the query operator and operands to determine the most appropriate estimation method. The function handles two main scenarios:

1. **Constant comparison**: When one operand is a constant value, it uses var_eq_const for more precise estimation based on histogram data and most common values.
2. **Non-constant comparison**: When both operands are variables or expressions, it uses var_eq_non_const for estimation.

The function also supports negation logic for inequality operators by first computing the equality selectivity and then converting it using the formula: '1.0 - eq_selectivity - nullfrac'. This approach leverages the existing equality estimation infrastructure for inequality operations.

## Parameters / Member Variables
- Standard PostgreSQL function arguments (PG_FUNCTION_ARGS):
  - : PlannerInfo pointer containing query planning context
  - : OID of the comparison operator
  - : List of operator arguments (left and right operands)
  - : Relation ID for variable references
  - : Collation information for the operation
- : Boolean flag indicating whether to compute inequality (true) or equality (false) selectivity

## Dependencies
- Functions called/Symbols referenced:
  - PG_GET_COLLATION
  - [get_negator](../g/get_negator.md)
  - get_restriction_variable
  - [var_eq_const](../v/var_eq_const.md)
  - [var_eq_non_const](../v/var_eq_non_const.md)
  - ReleaseVariableStats
  - DEFAULT_EQ_SEL (constant)
- Called from (representative examples):
  - [eqsel](eqsel.md)
  - [neqsel](../n/neqsel.md)

## Notes and Other Information
- Located in src/backend/utils/adt/selfuncs.c:237-295
- Returns a double value representing the estimated selectivity (0.0 to 1.0)
- Uses DEFAULT_EQ_SEL as fallback when operator negation fails or when expression structure is not recognized
- Automatically handles collation-aware comparisons
- Releases variable statistics memory using ReleaseVariableStats to prevent memory leaks
- The function's static nature indicates it's an internal implementation detail not exposed to external modules