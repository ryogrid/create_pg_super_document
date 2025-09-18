# int8_accum

## Location
src/backend/utils/adt/numeric.c: 5612 - 5631

## Overview
The int8_accum function is an accumulator function for 64-bit integer (int8/bigint) values in PostgreSQL's aggregate operations, managing accumulation state for statistical and mathematical aggregate functions.

## Definition
```c
Datum int8_accum(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as an accumulator for 64-bit integer values in PostgreSQL's aggregate framework. Unlike int4_accum, it uses NumericAggState exclusively (rather than the polymorphic PolyNumAggState) and always converts input values to numeric format for precise arithmetic operations. This ensures maximum precision for large integer calculations, particularly important for statistical functions like variance and standard deviation where intermediate calculations can involve very large numbers.

The function handles NULL values by skipping them during accumulation. On the first call, it initializes the accumulation state using makeNumericAggState. For subsequent calls, it converts the int64 input to numeric format and accumulates it using do_numeric_accum.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro containing:
  - Argument 0: NumericAggState pointer (accumulation state, can be NULL on first call)
  - Argument 1: int64 value to accumulate (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - [NumericAggState](../N/NumericAggState.md) (data structure)
  - [makeNumericAggState](../m/makeNumericAggState.md) (state initialization)
  - [do_numeric_accum](../d/do_numeric_accum.md) (numeric accumulation)
  - [int64_to_numeric](int64_to_numeric.md) (conversion utility)
  - PG_GETARG_INT64 (argument extraction macro)
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- Unlike int4_accum, this function always uses numeric arithmetic rather than optimizing with 128-bit integers, ensuring consistent precision for large values
- This is part of PostgreSQL's aggregate system specifically designed for bigint columns
- The function follows PostgreSQL's V1 calling convention using PG_FUNCTION_ARGS macro
- State is maintained across multiple calls during aggregate processing
- Commonly used in aggregate functions like AVG, STDDEV, and VARIANCE for bigint data types