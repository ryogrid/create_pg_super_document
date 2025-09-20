# int8_avg_accum

## Location
[src/backend/utils/adt/numeric.c:5808-5834](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L5808-L5834)

## Overview
Transition function for int8 input aggregation when computing averages that don't require sum of squares (sumX2).

## Definition

```c
Datum
int8_avg_accum(PG_FUNCTION_ARGS)
```
## Detailed Description
This function serves as an accumulator for PostgreSQL's average aggregate functions when processing 64-bit integer (int8/bigint) inputs. It's specifically designed for cases where only the sum and count are needed, not the sum of squares. The function maintains state using a PolyNumAggState structure and handles both 128-bit integer arithmetic (when available) and numeric arithmetic as fallback.

The function creates the accumulator state on the first call and updates it with each new input value. NULL values are ignored during accumulation.

## Parameters / Member Variables
- : PostgreSQL function calling convention macro that provides access to:
  - Arg 0: PolyNumAggState pointer (accumulator state, NULL on first call)
  - Arg 1: int64 value to accumulate (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  -  (structure type)
  -  (creates initial state)
  -  (128-bit integer accumulation when HAVE_INT128 is defined)
  -  (numeric accumulation fallback)
  -  (converts int64 to numeric type)
  -  (extracts int64 argument)
- Called from (representative examples):
  - No direct references found (likely referenced through PostgreSQL's aggregate function system)

## Notes and Other Information
- Uses conditional compilation with HAVE_INT128 to choose between 128-bit integer arithmetic and numeric arithmetic
- Part of PostgreSQL's polymorphic numeric aggregation system
- Optimized for average calculations that don't need variance/standard deviation (no sumX2)
- Returns a pointer to the updated state for chaining in aggregate operations