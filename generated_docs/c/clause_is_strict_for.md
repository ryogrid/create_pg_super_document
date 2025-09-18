# clause_is_strict_for

## Location
src/backend/optimizer/util/predtest.c: 1460 - 1661

## Overview
Determines if a clause returns NULL (or FALSE) when a specific subexpression yields NULL, implementing strictness analysis for PostgreSQL's predicate testing system.

## Definition


## Detailed Description
This function performs strictness analysis to prove whether a clause will definitely return NULL (or optionally FALSE) if a given subexpression evaluates to NULL. This is crucial for predicate testing logic where the optimizer needs to understand how NULL values propagate through expressions.

The function implements several layers of strictness detection:

1. **Direct equality**: If clause equals subexpr, it's trivially strict
2. **Strict operators/functions**: If the clause uses strict operators or functions, NULL inputs guarantee NULL outputs
3. **Type coercion strictness**: Various coercion operations (CoerceViaIO, ArrayCoerceExpr, ConvertRowtypeExpr, CoerceToDomain) preserve NULL values
4. **ScalarArrayOpExpr handling**: Special logic for array operations considering empty array edge cases
5. **NULL constants**: Direct NULL constants are always considered strict

The allow_false parameter provides flexibility for top-level boolean contexts where proving "not TRUE" is sufficient instead of proving strict NULL propagation.

## Parameters / Member Variables
- : The expression to analyze for strictness behavior
- : The subexpression that might be NULL, causing strictness
- : Whether proving FALSE result (not just NULL) is acceptable for top-level boolean expressions

## Dependencies
- Functions called/Symbols referenced:
  - IsA (for node type checking)
  - [equal](../e/equal.md) (for expression equality)
  - [is_opclause](../i/is_opclause.md), op_strict (for operator strictness)
  - [is_funcclause](../i/is_funcclause.md), func_strict (for function strictness)
  - linitial, lsecond (for list access)
  - DatumGetArrayTypeP, ArrayGetNItems, ARR_NDIM, ARR_DIMS (for array analysis)
  - [clause_is_strict_for](clause_is_strict_for.md) (recursive calls)
- Called from (representative examples):
  - [predicate_implied_by_simple_clause](../p/predicate_implied_by_simple_clause.md)
  - [predicate_refuted_by_simple_clause](../p/predicate_refuted_by_simple_clause.md)
  - [clause_is_strict_for](clause_is_strict_for.md) (recursive calls)

## Notes and Other Information
- Handles RelabelType nodes transparently by looking through them to match underlying expressions
- Assumes at least one input expression is immutable (verified by caller)
- Uses recursive analysis with allow_false=false for internal subexpressions to ensure actual NULL propagation
- Special handling for ScalarArrayOpExpr considers empty array edge cases where ANY returns false and ALL returns true
- The function is self-recursive, building up strictness proofs through expression trees
- Returns false for safety if inputs are NULL or unrecognized expression types