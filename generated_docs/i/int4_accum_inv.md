# int4_accum_inv

## Location
[src/backend/utils/adt/numeric.c:6015-6039](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L6015-L6039)

## Overview
Inverse accumulation function for 32-bit integer aggregate operations that removes a value from the running aggregate state.

## Definition

```c
Datum
int4_accum_inv(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function serves as the inverse accumulation function for aggregate operations involving 32-bit integers. It's used in window functions and moving aggregates where values need to be removed from the running state as the window slides. The function takes a  pointer and an integer value to remove from the accumulation state.

The function handles both 128-bit integer arithmetic (when available) and falls back to numeric operations for platforms without 128-bit support. It performs validation to ensure the state is not NULL and uses appropriate discard operations based on the compilation environment.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  - : PolyNumAggState pointer representing the current aggregate state
  - : int32 value to be removed from the accumulation

## Dependencies
- Functions called/Symbols referenced:
  -  (aggregate state structure)
  -  (128-bit integer discard operation)
  -  (numeric discard operation)
  -  (conversion function)
  -  (compilation flag)
- Called from (representative examples):
  - No direct references found in the indexed codebase

## Notes and Other Information
- The function includes error handling for NULL state conditions
- Uses conditional compilation with HAVE_INT128 to optimize for platforms with 128-bit integer support
- Part of PostgreSQL's aggregate function framework for window functions and moving aggregates
- Located in src/backend/utils/adt/numeric.c:6015-6039