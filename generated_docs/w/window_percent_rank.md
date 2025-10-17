# window_percent_rank

## Location
[src/backend/utils/adt/windowfuncs.c:261-287](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/windowfuncs.c#L261-L287)

## Overview
Implements the SQL PERCENT_RANK() window function, which returns the relative rank of each row as a fraction between 0 and 1, calculated as (rank - 1) / (total_rows - 1).

## Definition
```c
Datum window_percent_rank(PG_FUNCTION_ARGS)
```

## Detailed Description
The `window_percent_rank` function calculates the percentile rank of each row within a partition. It returns a floating-point value between 0 and 1 (inclusive) that represents the relative position of the current row among all rows in the partition.

The calculation follows the SQL standard formula: (RK - 1) / (NR - 1), where:
- RK is the current row's rank (1-based)
- NR is the total number of rows in the partition

Key characteristics:
- Returns 0.0 for the first row in the ordering
- Returns 1.0 for the last row in the ordering
- Returns 0.0 when there's only one row in the partition
- Ties (rows with identical ORDER BY values) receive the same percent rank
- Uses floating-point arithmetic for the final calculation

The function leverages the same ranking infrastructure as other ranking functions, using `rank_up()` to determine when the rank should change and maintaining state through the `rank_context` structure.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - PG_WINDOW_OBJECT (macro to get WindowObject)
  - [WinGetPartitionRowCount](../W/WinGetPartitionRowCount.md) (gets total number of rows in partition)
  - [rank_up](../r/rank_up.md) (utility function to check if rank should increase)
  - [WinGetPartitionLocalMemory](../W/WinGetPartitionLocalMemory.md) (allocates partition-local memory)
  - [WinGetCurrentPosition](../W/WinGetCurrentPosition.md) (gets current row position)
  - PG_RETURN_FLOAT8 (macro to return double-precision float result)
- Called from (representative examples):
  - No direct references found (likely called through function pointer in SQL execution)

## Notes and Other Information
- Part of PostgreSQL's window function implementation in windowfuncs.c
- Returns FLOAT8 (double-precision floating point) values
- Handles the edge case of single-row partitions by returning 0.0 as per SQL specification
- The rank is calculated based on the current position when rank changes occur
- Uses 1-based ranking internally but adjusts for 0-based percentage calculation
- Complies with SQL standard behavior for PERCENT_RANK() window function
- Suitable for statistical analysis and percentile calculations

## Simplified Source

```c
Datum window_percent_rank(PG_FUNCTION_ARGS)
{
    WindowObject winobj = PG_WINDOW_OBJECT();
    int64 totalrows = WinGetPartitionRowCount(winobj);

    // Get rank context and check if rank should increase
    bool should_increase = rank_up(winobj);
    rank_context *context = (rank_context *)
        WinGetPartitionLocalMemory(winobj, sizeof(rank_context));

    if (should_increase) {
        context->rank = WinGetCurrentPosition(winobj) + 1;
    }

    // Special case: return 0.0 for single row partitions
    if (totalrows <= 1) {
        PG_RETURN_FLOAT8(0.0);
    }

    // Calculate percent rank: (rank - 1) / (total_rows - 1)
    float8 percent = (float8)(context->rank - 1) / (float8)(totalrows - 1);
    PG_RETURN_FLOAT8(percent);
}
```