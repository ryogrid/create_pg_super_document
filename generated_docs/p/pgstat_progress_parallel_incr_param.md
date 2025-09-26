# pgstat_progress_parallel_incr_param

## Location
[src/backend/utils/activity/backend_progress.c:92-121](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/backend_progress.c#L92-L121)

## Overview
Provides progress increment functionality for parallel operations, allowing worker processes to send progress updates to the leader process for coordinated progress tracking.

## Definition
```c
void pgstat_progress_parallel_incr_param(int index, int64 incr)
```

## Detailed Description
This function is a parallel-aware variant of `pgstat_progress_incr_param` that handles progress tracking in parallel execution contexts. It automatically determines whether the calling process is a parallel worker or a leader process and takes appropriate action:

- **For parallel workers**: Sends a `PqMsg_Progress` message to the leader process containing the index and increment value. Workers cannot directly update the progress information since only the leader process owns the backend status entry.

- **For leader processes**: Directly calls `pgstat_progress_incr_param` to update the progress locally, since leaders have direct access to their backend status entry.

This design allows parallel operations to maintain consistent progress reporting across all participating processes while ensuring that only the leader process actually updates the visible progress information.

## Parameters / Member Variables
- `index`: The array index (0-based) in the progress parameter array to increment. Must be between 0 and PGSTAT_NUM_PROGRESS_PARAM-1
- `incr`: The 64-bit signed integer value to add to the current value at the specified index

## Dependencies
- Functions called/Symbols referenced:
  - IsParallelWorker (function)
  - [pq_beginmessage](pq_beginmessage.md) (function)
  - PqMsg_Progress (message type constant)
  - [pq_sendint32](pq_sendint32.md) (function)
  - [pq_sendint64](pq_sendint64.md) (function)
  - [pq_endmessage](pq_endmessage.md) (function)
  - [pgstat_progress_incr_param](pgstat_progress_incr_param.md) (function)
- Called from (representative examples):
  - [parallel_vacuum_process_one_index](parallel_vacuum_process_one_index.md) (parallel VACUUM operations)

## Notes and Other Information
- Uses a static StringInfoData buffer for message construction to avoid repeated memory allocations
- The message protocol uses 32-bit integers for the index and 64-bit integers for the increment value
- Leader processes handle incoming `PqMsg_Progress` messages in `HandleParallelMessage` function
- This function provides a unified interface that works correctly in both parallel and non-parallel contexts
- Essential for maintaining accurate progress reporting in parallel operations like parallel VACUUM
- The parallel messaging mechanism ensures that progress updates from all workers are properly aggregated by the leader