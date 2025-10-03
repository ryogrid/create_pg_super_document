# shm_mq_attach

## Location
[src/backend/storage/ipc/shm_mq.c:290-318](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/shm_mq.c#L290-L318)

## Overview
Attaches to a shared memory message queue to enable sending or receiving messages between processes.

## Definition
```c
shm_mq_handle *shm_mq_attach(shm_mq *mq, dsm_segment *seg, BackgroundWorkerHandle *handle)
```

## Detailed Description
This function creates and initializes a handle for accessing a shared memory message queue. The handle (`shm_mq_handle`) serves as the interface for subsequent operations on the queue. The function allocates the handle in the current memory context, which should have a lifetime at least as long as the message queue itself.

The function supports automatic cleanup by registering a detach callback when a dynamic shared memory segment is provided. It also enables cross-process communication by accepting a background worker handle for cases where communication needs to occur before both processes have fully attached.

Key initialization performed:
- Allocates and initializes the queue handle structure
- Sets up buffer management fields to zero/null state
- Records the current memory context for future allocations
- Registers automatic cleanup callback if a DSM segment is provided

## Parameters / Member Variables
- `mq`: Pointer to the shared memory queue structure to attach to
- `seg`: Optional dynamic shared memory segment; if provided, the queue will be automatically detached when this segment is detached
- `handle`: Optional background worker handle that must have bgw_notify_pid equal to the current process PID; allows communication before the other process attaches

## Dependencies
- Functions called/Symbols referenced:
  - [palloc](../p/palloc.md) (memory allocation)
  - [on_dsm_detach](../o/on_dsm_detach.md) (cleanup registration)
  - [shm_mq_detach_callback](shm_mq_detach_callback.md) (cleanup callback)
- Called from (representative examples):
  - [InitializeParallelDSM](../I/InitializeParallelDSM.md) (parallel query setup)
  - [ExecParallelSetupTupleQueues](../E/ExecParallelSetupTupleQueues.md) (parallel execution)
  - [ParallelWorkerMain](../P/ParallelWorkerMain.md) (worker process initialization)
  - [pa_setup_dsm](../p/pa_setup_dsm.md) (logical replication)

## Notes and Other Information
- The caller must ensure that either mq->mq_receiver or mq->mq_sender equals MyProc (verified by assertion)
- The memory context in effect during this call should persist for the lifetime of the queue
- [shm_mq_detach](shm_mq_detach.md)() should be called when done to properly clean up resources
- Future buffer allocations for incoming data will use the memory context active during this call
- The handle enables communication even before the counterpart process has attached

## Simplified Source

```c
shm_mq_handle *shm_mq_attach(shm_mq *mq, dsm_segment *seg, BackgroundWorkerHandle *handle) {
    // Allocate handle structure in current memory context
    shm_mq_handle *mqh = palloc(sizeof(shm_mq_handle));

    // Verify this process is either sender or receiver
    Assert(mq->mq_receiver == MyProc || mq->mq_sender == MyProc);

    // Initialize handle with queue and context info
    mqh->mqh_queue = mq;
    mqh->mqh_segment = seg;
    mqh->mqh_handle = handle;
    mqh->mqh_context = CurrentMemoryContext;

    // Initialize buffer management fields
    mqh->mqh_buffer = NULL;
    mqh->mqh_buflen = 0;
    mqh->mqh_consume_pending = 0;
    mqh->mqh_send_pending = 0;
    mqh->mqh_partial_bytes = 0;
    mqh->mqh_expected_bytes = 0;
    mqh->mqh_length_word_complete = false;
    mqh->mqh_counterparty_attached = false;

    // Register automatic cleanup if DSM segment provided
    if (seg != NULL)
        on_dsm_detach(seg, shm_mq_detach_callback, PointerGetDatum(mq));

    return mqh;
}
```