# int4_accum

## Location
src/backend/utils/adt/numeric.c: 5589 - 5611

## Overview
The int4_accum function is an accumulator function for integer (int4) values in PostgreSQL's aggregate operations. It manages the accumulation state for statistical and mathematical aggregate functions.

## Definition


## Detailed Description
This function serves as an accumulator for 32-bit integer values in PostgreSQL's aggregate framework. It maintains a PolyNumAggState structure that can handle both 128-bit integer arithmetic (when available) and numeric arithmetic for precise calculations. The function is typically used in aggregate operations like AVG, STDDEV, and VARIANCE for integer columns.

The function handles NULL values appropriately by skipping them during accumulation. On the first call, it initializes the accumulation state using makePolyNumAggState. For subsequent calls, it accumulates the input value using either 128-bit integer arithmetic (if HAVE_INT128 is defined) or falls back to numeric arithmetic for maximum precision.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro containing:
  - Argument 0: PolyNumAggState pointer (accumulation state, can be NULL on first call)
  - Argument 1: int32 value to accumulate (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - PolyNumAggState (data structure)
  - makePolyNumAggState (state initialization)
  - [do_int128_accum](../d/do_int128_accum.md) (128-bit integer accumulation when HAVE_INT128 is defined)
  - [do_numeric_accum](../d/do_numeric_accum.md) (numeric accumulation fallback)
  - [int64_to_numeric](int64_to_numeric.md) (conversion utility)
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- The function uses conditional compilation with HAVE_INT128 to optimize performance on platforms that support 128-bit integers
- This is part of PostgreSQL's polymorphic numeric aggregate system that can handle different input types efficiently
- The function follows PostgreSQL's V1 calling convention using PG_FUNCTION_ARGS macro
- State is maintained across multiple calls during aggregate processing and is automatically managed by the PostgreSQL aggregate framework