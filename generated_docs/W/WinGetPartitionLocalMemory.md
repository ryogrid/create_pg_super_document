# WinGetPartitionLocalMemory

## Location
src/backend/executor/nodeWindowAgg.c: 3170 - 3184

## Overview
Provides working memory that persists for the duration of partition processing in window functions, automatically allocated and zeroed on first call.

## Definition


## Detailed Description
This function serves as a memory management utility for window functions that need to maintain state across rows within a single partition. It implements a lazy allocation strategy where memory is allocated and zeroed only on the first call within a partition, with subsequent calls returning the same memory chunk. The allocated memory is tied to the partition context, ensuring automatic cleanup when partition processing completes. This design allows window functions to maintain partition-local state without manual memory management while ensuring proper isolation between different partitions.

## Parameters / Member Variables
- : WindowObject containing the window state and partition context information
- : Size in bytes of the memory block to allocate

## Dependencies
- Functions called/Symbols referenced:
  - WindowObjectIsValid
  - [MemoryContextAllocZero](../M/MemoryContextAllocZero.md)
- Called from (representative examples):
  - [rank_up](../r/rank_up.md)
  - [window_rank](../w/window_rank.md)
  - [window_dense_rank](../w/window_dense_rank.md)
  - [window_percent_rank](../w/window_percent_rank.md)
  - [window_cume_dist](../w/window_cume_dist.md)
  - [window_ntile](../w/window_ntile.md)

## Notes and Other Information
- Returns a pointer to the allocated memory block
- Memory is allocated from the partition context (winobj->winstate->partcontext)
- Memory is automatically zeroed on allocation
- Subsequent calls within the same partition return the same memory chunk
- Memory is automatically freed when partition processing ends
- Intended for partition-local state; use fcinfo->fn_extra for query-wide state
- Validates WindowObject before proceeding with allocation