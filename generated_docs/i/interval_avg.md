# interval_avg

## Location
[src/backend/utils/adt/timestamp.c:4167-4206](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/timestamp.c#L4167-L4206)

## Overview
Final function for the interval avg() aggregate that computes the average of interval values from accumulated state.

## Definition
```c
Datum interval_avg(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as the final function for PostgreSQL's interval avg() aggregate. It takes the accumulated IntervalAggState and computes the final average interval value. The function handles several special cases:

1. Returns NULL if no non-null inputs were processed
2. Handles infinite intervals by checking for mixed positive and negative infinities (which produces an error) or returns the appropriate infinite interval
3. For finite intervals, computes the average by dividing the sum by the count using interval_div()

The function properly handles the mathematical complexities of averaging intervals, including dealing with infinite values and ensuring proper error handling for undefined cases.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: PostgreSQL function argument macro containing:
  - Arg 0: IntervalAggState pointer (accumulated aggregate state)

## Dependencies
- Functions called/Symbols referenced:
  - [IntervalAggState](../I/IntervalAggState.md) (struct type)
  - IA_TOTAL_COUNT (macro for total count calculation)
  - Interval (struct type)
  - PG_ARGISNULL (PostgreSQL macro)
  - PG_GETARG_POINTER (PostgreSQL macro) 
  - PG_RETURN_NULL (PostgreSQL macro)
  - PG_RETURN_INTERVAL_P (PostgreSQL macro)
  - INTERVAL_NOEND (macro)
  - INTERVAL_NOBEGIN (macro)
  - DirectFunctionCall2 (PostgreSQL function call mechanism)
  - [interval_div](interval_div.md) (interval division function)
  - [IntervalPGetDatum](../I/IntervalPGetDatum.md) (conversion macro)
  - [Float8GetDatum](../F/Float8GetDatum.md) (conversion macro)
  - ereport (error reporting function)
  - [palloc](../p/palloc.md) (memory allocation function)
- Called from (representative examples):
  - No direct references found (likely used through PostgreSQL's aggregate function infrastructure)

## Notes and Other Information
- This is a final function in PostgreSQL's aggregate framework, called after all values have been accumulated
- Handles the complex case of infinite intervals, ensuring mathematical correctness
- Mixed positive and negative infinite intervals result in an "interval out of range" error
- Uses interval_div() for the actual division computation to maintain precision
- Part of PostgreSQL's interval data type aggregate operations