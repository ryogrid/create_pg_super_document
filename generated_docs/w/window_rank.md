# window_rank

## Location
[src/backend/utils/adt/windowfuncs.c:138-157](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/windowfuncs.c#L138-L157)

## Overview
Implements the RANK() window function, which assigns rank values to rows within a partition, with gaps in ranking when there are tied values.

## Definition
```c
Datum window_rank(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the SQL RANK() window function. Unlike ROW_NUMBER(), RANK() considers the ORDER BY clause values when assigning ranks. Rows with equal values in the ORDER BY columns receive the same rank, and subsequent ranks are skipped to account for the tied rows.

For example, if three rows tie for rank 2, they all get rank 2, and the next row gets rank 5 (not rank 3). This creates gaps in the ranking sequence.

The function uses the `rank_up()` utility function to determine when the rank should increase by comparing the current row with the previous row. If the rows are not peers (different ORDER BY values), the rank is updated to the current row position + 1. The rank context is stored in partition-local memory to maintain state across function calls within the same partition.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - PG_WINDOW_OBJECT
  - [rank_up](../r/rank_up.md)
  - [WinGetPartitionLocalMemory](../W/WinGetPartitionLocalMemory.md)
  - [WinGetCurrentPosition](../W/WinGetCurrentPosition.md)
  - PG_RETURN_INT64
  - [rank_context](../r/rank_context.md) (struct)
  - WindowObject (type)
- Called from (representative examples):
  - This is a PostgreSQL built-in function called directly from SQL queries

## Notes and Other Information
- Returns a 64-bit integer (int64) to handle large result sets
- The function is registered in PostgreSQL's system catalogs and can be called from SQL as RANK()
- Creates gaps in ranking when there are tied values, unlike DENSE_RANK() which doesn't create gaps
- Uses partition-local memory to maintain the current rank value across calls within a partition
- The rank value is only updated when `rank_up()` returns true, indicating that the current row is not a peer of the previous row
- For the first row in each partition, the rank is always 1 (handled by `rank_up()`)