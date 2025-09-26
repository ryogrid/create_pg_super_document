# is_strict_saop

## Location
[src/backend/optimizer/util/clauses.c:2026-2087](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/clauses.c#L2026-L2087)

## Overview
Determines whether a ScalarArrayOpExpr (scalar array operator expression) can be treated as strict with respect to NULL handling.

## Definition
static bool is_strict_saop(ScalarArrayOpExpr *expr, bool falseOK)

## Detailed Description
This function analyzes scalar array operator expressions to determine their strictness properties for NULL propagation analysis. The function handles two main cases of array operations:

**"foo op ALL array" expressions** are considered strict if:
1. The underlying operator is strict, AND
2. The array can be proven to be non-empty

**"foo op ANY array" expressions** are considered strict in the falseOK sense if the underlying operator is strict. When falseOK is false, they must meet the same requirements as ALL expressions.

The function can prove non-emptiness for two specific cases:
1. **Array constants**: By examining the actual array value and counting elements
2. **ARRAY[] constructs**: By checking if the ArrayExpr has non-empty elements and is not multidimensional

The falseOK parameter controls the strictness requirements: when true, returning "false" is acceptable as a strict result; when false, the function must guarantee an actual NULL result for NULL input.

## Parameters / Member Variables
- expr: The ScalarArrayOpExpr to analyze for strictness properties
- falseOK: Boolean flag indicating whether a "false" result can be considered strict (TRUE) or whether actual NULL result is required (FALSE)

## Dependencies
- Functions called/Symbols referenced:
  - [set_sa_opfuncid](../s/set_sa_opfuncid.md)
  - [func_strict](../f/func_strict.md)
  - lsecond
  - DatumGetArrayTypeP
  - [ArrayGetNItems](../A/ArrayGetNItems.md)
  - ARR_NDIM
  - ARR_DIMS
- Called from (representative examples):
  - [find_nonnullable_rels_walker](../f/find_nonnullable_rels_walker.md)
  - [find_nonnullable_vars_walker](../f/find_nonnullable_vars_walker.md)
  - max_parallel_hazard_context

## Notes and Other Information
- This is a static function internal to clauses.c
- The function is conservative and only proves non-emptiness for specific, analyzable cases
- [ScalarArrayOpExpr](../S/ScalarArrayOpExpr.md) represents operations like "value = ANY(array)" or "value = ALL(array)"
- The distinction between ANY and ALL operations is important for strictness analysis
- Empty arrays have special semantics that affect NULL propagation behavior
- The function is part of PostgreSQL's query optimization framework for handling NULL-aware operations