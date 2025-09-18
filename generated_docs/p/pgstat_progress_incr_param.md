# pgstat_progress_incr_param

## Location
[src/backend/utils/activity/backend_progress.c:70-91](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/backend_progress.c#L70-L91)

## Overview
Atomically increments a specific progress parameter by a given amount, providing an efficient way to update cumulative metrics during long-running PostgreSQL operations.

## Definition
```c
void pgstat_progress_incr_param(int index, int64 incr)
```

## Detailed Description
This function provides an atomic increment operation for progress parameters, which is particularly useful for tracking cumulative metrics that grow incrementally during operation execution. Instead of requiring the caller to read the current value, add the increment, and write it back, this function performs the entire read-modify-write operation atomically within the write activity protection.

This is commonly used for metrics like the number of tuples processed, blocks scanned, or bytes transferred, where the operation naturally accumulates progress over time. The atomic nature ensures that concurrent reads of the progress information see consistent values even during updates.

## Parameters / Member Variables
- `index`: The array index (0-based) in the progress parameter array to increment. Must be between 0 and PGSTAT_NUM_PROGRESS_PARAM-1
- `incr`: The 64-bit signed integer value to add to the current value at the specified index. Can be negative to decrement

## Dependencies
- Functions called/Symbols referenced:
  - [PgBackendStatus](../P/PgBackendStatus.md) (struct type)
  - PGSTAT_NUM_PROGRESS_PARAM (constant)
  - PGSTAT_BEGIN_WRITE_ACTIVITY (macro)
  - PGSTAT_END_WRITE_ACTIVITY (macro)
- Called from (representative examples):
  - [DefineIndex](../D/DefineIndex.md) (CREATE INDEX progress increments)
  - [HandleParallelMessage](../H/HandleParallelMessage.md) (parallel worker progress aggregation)
  - [pgstat_progress_parallel_incr_param](pgstat_progress_parallel_incr_param.md) (parallel progress coordination)

## Notes and Other Information
- Includes an assertion to validate that the index is within the valid range [0, PGSTAT_NUM_PROGRESS_PARAM)
- Returns immediately if the backend entry is not available or progress tracking is disabled
- Uses atomic write operations to ensure the read-modify-write operation is atomic
- Supports both positive increments (most common) and negative decrements (less common)
- More efficient than separate read and update operations for cumulative metrics
- Particularly useful in loops where progress accumulates incrementally
- Used less frequently than pgstat_progress_update_param but provides better semantics for cumulative counters