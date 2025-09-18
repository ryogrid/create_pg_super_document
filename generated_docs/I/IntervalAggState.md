# IntervalAggState

## Location
src/backend/utils/adt/timestamp.c: 78 - 85

## Overview
IntervalAggState is a PostgreSQL data structure used as the transition state for interval aggregate functions, maintaining counts and sums of finite and infinite interval values during aggregation operations.

## Definition


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
  - makeIntervalAggState (allocation/initialization)
  - interval_avg_accum (accumulation for AVG aggregate)
  - interval_avg_combine (combining partial states)
  - interval_avg_serialize/deserialize (serialization for parallel processing)
  - interval_avg_accum_inv (inverse accumulation for moving windows)
  - interval_avg (final result computation for AVG)
  - interval_sum (final result computation for SUM)
  - do_interval_accum (core accumulation logic)
  - do_interval_discard (removal logic for moving windows)

## Notes and Other Information
- The structure is allocated in the aggregate's memory context to ensure proper lifetime management during query execution
- The separation of finite and infinite interval counts allows proper handling of mathematical edge cases involving infinity
- The IA_TOTAL_COUNT() macro provides the total count of all intervals (finite + infinite): 
- This design supports PostgreSQL's parallel aggregation feature through serialization/deserialization functions
- The structure supports both forward and inverse operations, enabling its use in moving window aggregates
- Memory management follows PostgreSQL's palloc/pfree model within the aggregate context