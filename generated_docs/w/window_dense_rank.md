# window_dense_rank

## Location
[src/backend/utils/adt/windowfuncs.c:200-219](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/windowfuncs.c#L200-L219)

## Overview
Implements the SQL DENSE_RANK() window function, which assigns a dense rank to each row within a partition based on the ORDER BY clause, incrementing by 1 when key columns change.

## Definition
```c
Datum window_dense_rank(PG_FUNCTION_ARGS)
```

## Detailed Description
The `window_dense_rank` function calculates dense ranking for window functions. Unlike regular RANK(), dense ranking assigns consecutive integers starting from 1, incrementing by exactly 1 whenever the ordering keys change between rows. This means there are no gaps in the rank sequence.

The function uses partition-local memory to maintain a `rank_context` structure that tracks the current rank value. It calls the utility function `rank_up()` to determine whether the rank should be incremented based on whether the current row's ordering keys differ from the previous row's keys.

Key characteristics:
- Returns consecutive integers (1, 2, 3, ...) without gaps
- Rank increases by exactly 1 when ORDER BY values change
- Multiple rows with identical ORDER BY values receive the same rank
- Uses partition-local memory for rank state management

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - PG_WINDOW_OBJECT (macro to get WindowObject)
  - [rank_up](../r/rank_up.md) (utility function to check if rank should increase)
  - [WinGetPartitionLocalMemory](../W/WinGetPartitionLocalMemory.md) (allocates partition-local memory)
  - PG_RETURN_INT64 (macro to return 64-bit integer result)
- Called from (representative examples):
  - No direct references found (likely called through function pointer in SQL execution)

## Notes and Other Information
- Part of PostgreSQL's window function implementation in windowfuncs.c
- Uses rank_context structure to maintain current rank state
- Integrates with PostgreSQL's window function framework
- Returns INT64 to handle large result sets
- The rank starts at 1 for the first row in each partition
- Dense ranking ensures no gaps in the sequence, making it suitable for scenarios requiring consecutive numbering

## Simplified Source

```c
Datum window_dense_rank(PG_FUNCTION_ARGS)
{
    WindowObject winobj = PG_WINDOW_OBJECT();
    rank_context *context = (rank_context *)
        WinGetPartitionLocalMemory(winobj, sizeof(rank_context));

    // Check if rank should increase for non-peer rows
    bool should_increase = rank_up(winobj);

    if (should_increase) {
        // Increment rank by 1 (no gaps, dense ranking)
        context->rank++;
    }

    PG_RETURN_INT64(context->rank);
}
```