# window_cume_dist

## Location
[src/backend/utils/adt/windowfuncs.c:330-370](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/windowfuncs.c#L330-L370)

## Overview
Implements the SQL CUME_DIST() window function, which returns the cumulative distribution as a fraction between 0 and 1, calculated as the number of rows preceding or peer to the current row divided by the total number of rows.

## Definition
```c
Datum window_cume_dist(PG_FUNCTION_ARGS)
```

## Detailed Description
The `window_cume_dist` function calculates the cumulative distribution of each row within a partition. It returns a floating-point value between 0 and 1 (inclusive) that represents the relative position of the current row when considering all rows that have values less than or equal to the current row's ORDER BY values.

The calculation follows the SQL standard formula: NP / NR, where:
- NP is the number of rows preceding or peer to the current row
- NR is the total number of rows in the partition

Key characteristics:
- Always returns a value > 0 (minimum is 1/total_rows for the first unique value)
- Returns 1.0 for the last row(s) in the ordering
- Rows with identical ORDER BY values (peers) receive the same cumulative distribution value
- The function counts forward from the current position to find all peer rows

The implementation uses a forward-scanning approach: when the rank changes or for the first row, it scans ahead to count all rows that are peers to the current row, updating the context rank to include the count of all peer rows.

## Parameters / Member Variables
- This function follows the PostgreSQL function calling convention (PG_FUNCTION_ARGS)
- Accesses the WindowObject through PG_WINDOW_OBJECT() macro

## Dependencies
- Functions called/Symbols referenced:
  - PG_WINDOW_OBJECT (macro to get WindowObject)
  - [WinGetPartitionRowCount](../W/WinGetPartitionRowCount.md) (gets total number of rows in partition)
  - [rank_up](../r/rank_up.md) (utility function to check if rank should increase)
  - [WinGetPartitionLocalMemory](../W/WinGetPartitionLocalMemory.md) (allocates partition-local memory)
  - [WinGetCurrentPosition](../W/WinGetCurrentPosition.md) (gets current row position)
  - [WinRowsArePeers](../W/WinRowsArePeers.md) (checks if two rows have equivalent ORDER BY values)
  - PG_RETURN_FLOAT8 (macro to return double-precision float result)
- Called from (representative examples):
  - No direct references found (likely called through function pointer in SQL execution)

## Notes and Other Information
- Part of PostgreSQL's window function implementation in windowfuncs.c
- Returns FLOAT8 (double-precision floating point) values
- Uses forward scanning to count peer rows, which may be less efficient than other ranking functions for large partitions with many peers
- The rank context stores the count of rows up to and including all peers of the current row
- Different from PERCENT_RANK() in that it includes the current row and all peers in the numerator
- Suitable for statistical analysis and percentile calculations
- Complies with SQL standard behavior for CUME_DIST() window function
- The scanning approach ensures accurate peer counting but may impact performance with large numbers of peer rows