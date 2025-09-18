# addToSimpleStats

## Location
[src/bin/pgbench/pgbench.c:1403-1417](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pgbench/pgbench.c#L1403-L1417)

## Overview
Accumulates a single numerical value into a SimpleStats structure, updating all statistical fields (count, min, max, sum, sum of squares).

## Definition
```c
static void addToSimpleStats(SimpleStats *ss, double val)
```

## Detailed Description
This function adds a new data point to the running statistics maintained in a SimpleStats structure. It performs the following updates:

1. **Min/Max tracking**: Updates minimum and maximum values if this is the first value (count == 0) or if the new value establishes a new extreme
2. **Count increment**: Increases the count of observed values
3. **Sum accumulation**: Adds the value to the running sum
4. **Sum of squares**: Adds the squared value to sum2 for variance/standard deviation calculations

This incremental approach allows efficient computation of statistical measures without storing all individual data points.

## Parameters / Member Variables
- `ss`: Pointer to the SimpleStats structure to update
- `val`: The numerical value to add to the statistics

## Dependencies
- Functions called/Symbols referenced:
  - SimpleStats (structure type)
- Called from (representative examples):
  - [accumStats](accumStats.md)
  - [advanceConnectionState](advanceConnectionState.md)

## Notes and Other Information
- Handles the first value case correctly by checking count == 0 for min/max initialization
- Maintains data needed for mean (sum/count) and variance (uses sum2 for variance calculation)
- Part of pgbench's real-time statistics collection during benchmark execution
- Does not perform any validation on the input value - assumes caller provides valid doubles
- Located in src/bin/pgbench/pgbench.c:1403-1417