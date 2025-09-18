# ExecEvalMinMax

## Location
[src/backend/executor/execExprInterp.c:3120-3172](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execExprInterp.c#L3120-L3172)

## Overview
Evaluates GREATEST() or LEAST() expressions by comparing pre-evaluated arguments using a comparison function and selecting the appropriate value based on the operation type.

## Definition
```c
void ExecEvalMinMax(ExprState *state, ExprEvalStep *op)
```

## Detailed Description
ExecEvalMinMax implements the evaluation logic for PostgreSQL's GREATEST() and LEAST() functions. Note that these are distinct from aggregate MIN()/MAX() functions. The function operates on a set of pre-evaluated expressions, comparing them pairwise using a type-specific comparison function to determine the greatest or least value.

The function follows SQL NULL semantics: NULL values are ignored during comparison, and the result is NULL only if all input values are NULL. The comparison is performed using a prepared FunctionCallInfo structure that contains the appropriate comparison function for the data type being processed.

The algorithm iterates through all values, maintaining the current min/max candidate and updating it when a more extreme value is found based on the operation type (IS_GREATEST or IS_LEAST).

## Parameters / Member Variables
- `state`: ExprState containing the overall expression evaluation context (currently unused)
- `op`: ExprEvalStep containing the specific operation data including:
  - `op->d.minmax.values`: Array of pre-evaluated Datum values to compare
  - `op->d.minmax.nulls`: Array of boolean flags indicating NULL status for each value
  - `op->d.minmax.fcinfo_data`: FunctionCallInfo structure for the comparison function
  - `op->d.minmax.op`: MinMaxOp enum indicating whether this is GREATEST or LEAST
  - `op->d.minmax.nelems`: Number of elements in the values/nulls arrays
  - `op->resvalue`: Pointer to store the result value
  - `op->resnull`: Pointer to store the result NULL flag

## Dependencies
- Functions called/Symbols referenced:
  - FunctionCallInvoke: Executes the type-specific comparison function
  - [DatumGetInt32](../D/DatumGetInt32.md): Extracts integer comparison result from Datum
  - IS_LEAST: MinMaxOp constant for LEAST() operation
  - IS_GREATEST: MinMaxOp constant for GREATEST() operation
- Called from (representative examples):
  - [ExecInterpExpr](ExecInterpExpr.md): Main expression interpreter dispatch function
  - [FunctionReturningBool](../F/FunctionReturningBool.md): JIT compilation context

## Notes and Other Information
- This function handles GREATEST()/LEAST() functions, not aggregate MIN()/MAX() operations
- Follows SQL standard NULL handling: NULLs are ignored, result is NULL only if all inputs are NULL
- Uses type-specific comparison functions through the function manager for proper ordering semantics
- The comparison function is expected to return negative, zero, or positive integers for less-than, equal, or greater-than relationships
- Performance is optimized by pre-evaluating all arguments and using prepared function call information
- Part of PostgreSQL's compiled expression evaluation system for efficient execution