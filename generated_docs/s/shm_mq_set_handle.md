# shm_mq_set_handle

## Location
[src/backend/storage/ipc/shm_mq.c:319-328](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/shm_mq.c#L319-L328)

## Overview
Associates a BackgroundWorkerHandle with an existing shared memory queue handle after attachment.

## Definition
```c
void shm_mq_set_handle(shm_mq_handle *mqh, BackgroundWorkerHandle *handle)
```

## Detailed Description
This function allows setting a BackgroundWorkerHandle on a shared memory queue handle that was previously created without one. This is useful in scenarios where the queue handle is created first (via `shm_mq_attach` with a NULL handle parameter) and the background worker handle becomes available later.

The function performs a simple assignment operation but includes an assertion to ensure that no handle was previously set, preventing accidental overwrites of existing background worker associations.

## Parameters / Member Variables
- `mqh`: Pointer to an existing shared memory queue handle
- `handle`: BackgroundWorkerHandle to associate with the queue handle; must have bgw_notify_pid equal to the current process PID

## Dependencies
- Functions called/Symbols referenced:
  - Assert (validation macro)
- Called from (representative examples):
  - [LaunchParallelWorkers](../L/LaunchParallelWorkers.md) (parallel query execution)
  - [ExecParallelCreateReaders](../E/ExecParallelCreateReaders.md) (parallel execution setup)

## Notes and Other Information
- The function requires that no handle was previously set (mqh->mqh_handle must be NULL)
- This provides the same functionality as passing the handle directly to shm_mq_attach, but allows for deferred association
- Typically used in parallel query execution where worker handles become available after initial queue setup
- The handle enables the queue to communicate with background worker processes even before they fully attach