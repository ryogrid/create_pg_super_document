# do_numeric_accum

## Location
[src/backend/utils/adt/numeric.c:4873-4942](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L4873-L4942)

## Overview
Accumulates a new numeric input value into a NumericAggState structure, handling special values and maintaining running sums for aggregate functions.

## Definition

```c
static void
do_numeric_accum(NumericAggState *state, Numeric newval)
```
## Detailed Description
This static function performs the core accumulation logic for numeric aggregate functions. It handles special numeric values (positive infinity, negative infinity, NaN) by maintaining separate counters for each type. For regular numeric values, it tracks the maximum decimal scale encountered (needed for inverse transitions), converts the input to variable format, optionally computes the square of the value if required, and then accumulates the value and its square (if needed) into running sums. The function carefully manages memory contexts, performing calculations in short-lived contexts but storing accumulated results in the aggregate context to ensure proper memory management throughout the aggregate operation.

## Parameters / Member Variables
- : Pointer to NumericAggState structure containing aggregate state (counters, sums, context information)
- : The new numeric value to accumulate into the aggregate state

## Dependencies
- Functions called/Symbols referenced:
  -  - Check if numeric value is special (NaN, infinity)
  -  - Check for positive infinity
  -  - Check for negative infinity
  -  - Convert numeric to NumericVar format
  -  - [Initialize](../I/Initialize.md) NumericVar structure
  -  - Multiply two NumericVar values
  -  - Add value to accumulated sum
  -  - Switch memory contexts
- Called from (representative examples):
  -  - Standard numeric accumulation function
  -  - [Numeric](../N/Numeric.md) average accumulation
  -  - 16-bit integer accumulation
  -  - 32-bit integer accumulation
  -  - 64-bit integer accumulation
  -  - 64-bit integer average accumulation

## Notes and Other Information
- Declared as static, limiting visibility to numeric.c file
- Maintains separate counters for NaN, positive infinity, and negative infinity
- Tracks maximum decimal scale for supporting inverse aggregate transitions
- Uses memory context switching to ensure proper allocation of persistent vs temporary data
- Computes sum of squares only when requested (controlled by state->calcSumX2 flag)
- Essential building block for PostgreSQL's numeric aggregate functions (SUM, AVG, etc.)
- Carefully handles precision by using appropriate decimal scales for squared values
- Located in src/backend/utils/adt/numeric.c:4873-4942