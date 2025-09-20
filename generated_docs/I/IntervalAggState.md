# IntervalAggState

## Location
[src/backend/utils/adt/timestamp.c:78-85](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/timestamp.c#L78-L85)

## Overview
IntervalAggState is a PostgreSQL data structure used as the transition state for interval aggregate functions, maintaining counts and sums of finite and infinite interval values during aggregation operations.

## Definition

```c
typedef struct IntervalAggState
{
	int64		N;				/* count of finite intervals processed */
	Interval	sumX;			/* sum of finite intervals processed */
	/* These counts are *not* included in N!  Use IA_TOTAL_COUNT() as needed */
	int64		pInfcount;		/* count of +infinity intervals */
	int64		nInfcount;		/* count of -infinity intervals */
} IntervalAggState;
```
## Detailed Description
The IntervalAggState structure serves as the transition datatype for PostgreSQL's interval aggregate functions such as AVG() and SUM() when applied to interval data types. This structure is declared as internal and represents a pointer to memory allocated in the aggregate context.

The structure efficiently handles both finite and infinite interval values during aggregation. Finite intervals are accumulated in the  field while their count is maintained in . Infinite values (both positive and negative infinity) are counted separately but not included in the sum, as mathematical operations with infinity require special handling.

The structure is designed to support incremental aggregation operations, allowing for efficient accumulation, combination of partial results (for parallel aggregation), and final result computation. It also supports inverse operations for moving window aggregates.

## Parameters / Member Variables
- : Count of finite (non-infinite) interval values that have been processed and included in the sum
- : Accumulated sum of all finite interval values processed so far  
- : Count of positive infinity interval values encountered (not included in N)
- : Count of negative infinity interval values encountered (not included in N)

## Dependencies
- Functions called/Symbols referenced:
  - Interval (PostgreSQL's interval data type)
  - IA_TOTAL_COUNT (macro to get total count including infinities)

- Called from (representative examples):
  - [makeIntervalAggState](../m/makeIntervalAggState.md) (allocation/initialization)
  - [interval_avg_accum](../i/interval_avg_accum.md) (accumulation for AVG aggregate)
  - [interval_avg_combine](../i/interval_avg_combine.md) (combining partial states)
  - [interval_avg_serialize](../i/interval_avg_serialize.md)/deserialize (serialization for parallel processing)
  - [interval_avg_accum_inv](../i/interval_avg_accum_inv.md) (inverse accumulation for moving windows)
  - [interval_avg](../i/interval_avg.md) (final result computation for AVG)
  - [interval_sum](../i/interval_sum.md) (final result computation for SUM)
  - [do_interval_accum](../d/do_interval_accum.md) (core accumulation logic)
  - [do_interval_discard](../d/do_interval_discard.md) (removal logic for moving windows)

## Notes and Other Information
- The structure is allocated in the aggregate's memory context to ensure proper lifetime management during query execution
- The separation of finite and infinite interval counts allows proper handling of mathematical edge cases involving infinity
- The IA_TOTAL_COUNT() macro provides the total count of all intervals (finite + infinite): 
- This design supports PostgreSQL's parallel aggregation feature through serialization/deserialization functions
- The structure supports both forward and inverse operations, enabling its use in moving window aggregates
- Memory management follows PostgreSQL's palloc/pfree model within the aggregate context