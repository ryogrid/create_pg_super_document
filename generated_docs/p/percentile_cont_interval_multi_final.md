# percentile_cont_interval_multi_final

## Location
[src/backend/utils/adt/orderedsetaggs.c:1019-1032](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/orderedsetaggs.c#L1019-L1032)

## Overview
Implements the final phase of the PostgreSQL aggregate function , which computes continuous percentiles for interval data types.

## Definition

```c
Datum
percentile_cont_interval_multi_final(PG_FUNCTION_ARGS)
```
## Detailed Description
This function serves as the finalization function for the  ordered-set aggregate when applied to interval data types with multiple percentile values. It acts as a thin wrapper around , providing interval-specific type information and the appropriate linear interpolation function () for continuous percentile calculation.

The function handles the computation of multiple percentiles simultaneously from a sorted set of interval values, performing linear interpolation when the desired percentile falls between two data points.

## Parameters / Member Variables
- : Standard PostgreSQL function call information structure containing aggregate state and percentile array parameter

## Dependencies
- Functions called/Symbols referenced:
  - : Core implementation for multi-percentile continuous calculations
  - : Type alignment constant for interval type
  - : Linear interpolation function for interval values
- Called from (representative examples):
  - PostgreSQL aggregate execution framework (no direct callers found in indexed code)

## Notes and Other Information
- This is part of PostgreSQL's ordered-set aggregate implementation for statistical functions
- The function is registered as an aggregate final function in the system catalogs
- Hard-wired type information is provided: 16 bytes length, pass-by-reference, double alignment
- Uses  to specify the expected interval data type
- The actual computation logic is delegated to the common implementation function to avoid code duplication across different data types