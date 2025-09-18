# pa_free_worker_info

## Location
src/backend/replication/logical/applyparallelworker.c: 595 - 621

## Overview
Frees parallel apply worker information structure and cleans up associated resources including message queues, serialized files, and dynamic shared memory segments.

## Definition
```c
static void pa_free_worker_info(ParallelApplyWorkerInfo *winfo)
```

## Detailed Description
This function performs complete cleanup of a ParallelApplyWorkerInfo structure and all its associated resources. It systematically detaches from shared message queues, cleans up any serialized transaction files, detaches from dynamic shared memory segments, removes the worker from the pool, and finally frees the memory. This ensures no resource leaks when a parallel apply worker is completely shut down.

The function handles cleanup of:
1. Main message queue handle (mq_handle) for communication with the worker
2. Error message queue handle (error_mq_handle) for error reporting
3. Serialized transaction files if the worker had serialized changes
4. Dynamic shared memory segment used for worker communication
5. Removal from the global ParallelApplyWorkerPool list
6. The worker info structure itself

## Parameters / Member Variables
- `winfo`: Pointer to ParallelApplyWorkerInfo structure to be freed. Must be non-NULL and contain the worker's resource handles.

## Dependencies
- Functions called/Symbols referenced:
  - [shm_mq_detach](../s/shm_mq_detach.md) (detach from shared message queues)
  - [stream_cleanup_files](../s/stream_cleanup_files.md) (clean up serialized transaction files)
  - [dsm_detach](../d/dsm_detach.md) (detach from dynamic shared memory)
  - [list_delete_ptr](../l/list_delete_ptr.md) (remove from worker pool list)
  - [pfree](pfree.md) (free memory)
- Called from (representative examples):
  - [pa_launch_parallel_worker](pa_launch_parallel_worker.md) (cleanup on worker launch failure)
  - [pa_free_worker](pa_free_worker.md) (when stopping a worker)

## Notes and Other Information
- Function includes assertion to ensure winfo is not NULL
- Safely handles cases where some resources might not be allocated (null checks)
- Serialized files are only cleaned up if the worker actually serialized changes
- Part of the resource management infrastructure for parallel apply workers
- Essential for preventing resource leaks in the logical replication subsystem