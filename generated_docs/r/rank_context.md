# rank_context

## Location
[src/backend/utils/adt/windowfuncs.c:24-27](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/windowfuncs.c#L24-L27)

## Overview
The rank_context struct stores ranking process information for window functions in PostgreSQL, maintaining the current rank value across window function evaluations.

## Definition
```c
typedef struct rank_context
{
    int64       rank;           /* current rank */
} rank_context;
```

## Detailed Description
The rank_context structure is a simple context object used by PostgreSQL window ranking functions (rank(), dense_rank(), percent_rank(), and cume_dist()) to maintain state information across multiple calls within a window partition. It serves as persistent storage for the current rank value, which is allocated in partition-local memory and persists throughout the evaluation of a window partition.

The structure is designed to work with PostgreSQL window function framework, specifically using WinGetPartitionLocalMemory() to allocate and retrieve the context for each partition. This ensures that ranking calculations are correctly isolated between different window partitions and that the rank state is maintained across multiple function calls within the same partition.

## Parameters / Member Variables
- `rank`: A 64-bit signed integer that stores the current rank value. This field tracks the ranking position within the current window partition and is updated as the window function processes each row.

## Dependencies
- Functions called/Symbols referenced:
  - (None - this is a simple struct definition)
  
- Called from (representative examples):
  - [rank_up](rank_up.md) (utility function at line 53, 55, 56)
  - [window_rank](../w/window_rank.md) (main rank function at line 141, 145, 146)
  - [window_dense_rank](../w/window_dense_rank.md) (dense rank function at line 203, 207, 208)
  - [window_percent_rank](../w/window_percent_rank.md) (percent rank function at line 264, 271, 272)
  - [window_cume_dist](../w/window_cume_dist.md) (cumulative distribution function at line 333, 340, 341)

## Notes and Other Information
- This structure is allocated using PostgreSQL partition-local memory management (WinGetPartitionLocalMemory), ensuring proper memory lifecycle management within window partitions
- The rank field is initialized to 0, and the first call to any ranking function sets it to 1 (first row rank)
- Different ranking functions use this context differently:
  - rank(): Sets rank to current row position + 1 when rows are not peers
  - dense_rank(): Increments rank by 1 when rows are not peers
  - percent_rank() and cume_dist(): Use the rank for statistical calculations
- The structure is defined in src/backend/utils/adt/windowfuncs.c:24-27
- This context works in conjunction with the rank_up() utility function which determines when the rank should be updated based on peer row comparisons