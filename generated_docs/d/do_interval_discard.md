# do_interval_discard

## Location
[src/backend/utils/adt/timestamp.c:3971-4001](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/timestamp.c#L3971-L4001)

## Overview
Removes a given interval value from the aggregated state used in interval average calculations, supporting PostgreSQL's inverse aggregate functions for sliding window operations.

## Definition


## Detailed Description
This static function is responsible for removing an interval value from the accumulated state during inverse aggregation operations. It handles three types of interval values: negative infinite intervals (NOBEGIN), positive infinite intervals (NOEND), and finite intervals. For infinite intervals, it decrements the respective infinity counters without affecting the finite value count. For finite intervals, it decrements the count N and subtracts the value from the running sum using finite_interval_mi. When all values are discarded (N reaches 0), it resets the sum to zero to maintain numerical stability.

## Parameters / Member Variables
- : Pointer to IntervalAggState structure containing aggregation counters and running sum
- : Pointer to the Interval value to be removed from the aggregated state

## Dependencies
- Functions called/Symbols referenced:
  - INTERVAL_IS_NOBEGIN (macro for checking negative infinity)
  - INTERVAL_IS_NOEND (macro for checking positive infinity) 
  - [finite_interval_mi](../f/finite_interval_mi.md) (function for interval subtraction)
- Called from (representative examples):
  - [interval_avg_accum_inv](../i/interval_avg_accum_inv.md)

## Notes and Other Information
- This function is part of PostgreSQL's inverse aggregate function framework, enabling efficient sliding window calculations
- Infinite intervals are tracked separately and do not contribute to the finite value count N
- The function includes safety measures to reset the sum when all values are discarded to prevent floating-point accumulation errors
- Used specifically for interval average calculations in window functions and moving aggregates