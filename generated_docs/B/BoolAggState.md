# BoolAggState

## Location
src/backend/utils/adt/bool.c: 304 - 308

## Overview
BoolAggState is a PostgreSQL structure used for maintaining state during boolean aggregate operations, tracking both the total number of non-null boolean values processed and the count of true values.

## Definition
```c
typedef struct BoolAggState
{
    int64       aggcount;       /* number of non-null values aggregated */
    int64       aggtrue;        /* number of values aggregated that are true */
} BoolAggState;
```

## Detailed Description
BoolAggState serves as the internal state structure for PostgreSQL boolean aggregate functions like `bool_and()` and `bool_or()`. It maintains running counts that enable efficient computation of aggregate boolean operations over sets of data. The structure is allocated in aggregate memory context and persists throughout the aggregation process, accumulating statistics that are used by final functions like `bool_alltrue` and `bool_anytrue` to produce the final aggregate result.

The design follows PostgreSQL's general aggregate function pattern where a state structure accumulates intermediate results during the scan phase, and a final function computes the result from the accumulated state.

## Parameters / Member Variables
- `aggcount`: Total number of non-null boolean values processed during aggregation
- `aggtrue`: Number of boolean values that evaluated to true during aggregation

## Dependencies
- Functions called/Symbols referenced:
  - int64 (data type)
- Called from (representative examples):
  - [makeBoolAggState](../m/makeBoolAggState.md) (creates and initializes instances)
  - [bool_accum](../b/bool_accum.md) (accumulates boolean values into the state)
  - [bool_accum_inv](../b/bool_accum_inv.md) (removes boolean values from state for window functions)
  - [bool_alltrue](../b/bool_alltrue.md) (final function that returns true if all values were true)
  - [bool_anytrue](../b/bool_anytrue.md) (final function that returns true if any value was true)

## Notes and Other Information
- Used exclusively for boolean aggregate operations in PostgreSQL
- Memory allocation handled in aggregate memory context via `makeBoolAggState()`
- Supports both forward aggregation (`bool_accum`) and inverse aggregation (`bool_accum_inv`) for sliding window operations
- The ratio of `aggtrue` to `aggcount` provides the foundation for boolean aggregate logic: all-true requires `aggtrue == aggcount`, any-true requires `aggtrue > 0`
- Part of PostgreSQL's boolean data type implementation in `src/backend/utils/adt/bool.c`