# mergeSimpleStats

## Location
[src/bin/pgbench/pgbench.c:1418-1433](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pgbench/pgbench.c#L1418-L1433)

## Overview
Merges two SimpleStats objects by combining their statistical data, allowing aggregation of statistics collected from multiple sources.

## Definition
```c
static void mergeSimpleStats(SimpleStats *acc, SimpleStats *ss)
```

## Detailed Description
This function combines the statistical data from two SimpleStats structures, merging the source (`ss`) into the accumulator (`acc`). The merge operation correctly handles all statistical fields:

1. **Min/Max values**: Takes the overall minimum and maximum across both datasets
2. **Count combination**: Adds the counts from both structures  
3. **Sum aggregation**: Combines the sums from both datasets
4. **Sum of squares**: Combines the sum2 values for proper variance calculation

This enables aggregation of statistics from multiple threads, time periods, or measurement sources while maintaining mathematical correctness for statistical computations.

## Parameters / Member Variables
- `acc`: Pointer to the accumulator SimpleStats structure that receives the merged data
- `ss`: Pointer to the source SimpleStats structure to merge into the accumulator

## Dependencies
- Functions called/Symbols referenced:
  - SimpleStats (structure type)
- Called from (representative examples):
  - [printProgressReport](../p/printProgressReport.md)
  - [main](main.md)

## Notes and Other Information
- Handles empty accumulator case correctly by checking acc->count == 0
- After merging, the accumulator contains statistics as if all data points from both sources were collected together
- Essential for multi-threaded pgbench scenarios where each thread collects separate statistics
- Does not modify the source SimpleStats structure (`ss`)
- Located in src/bin/pgbench/pgbench.c:1418-1433

## Simplified Source

```c
static void mergeSimpleStats(SimpleStats *acc, SimpleStats *ss) {
    // Update overall min/max from both datasets
    if (acc->count == 0 || ss->min < acc->min)
        acc->min = ss->min;
    if (acc->count == 0 || ss->max > acc->max)
        acc->max = ss->max;

    // Combine counts, sums, and sum of squares
    acc->count += ss->count;
    acc->sum += ss->sum;
    acc->sum2 += ss->sum2;
}
```