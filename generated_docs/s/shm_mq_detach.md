# shm_mq_detach

## Location
[src/backend/storage/ipc/shm_mq.c:843-881](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/shm_mq.c#L843-L881)

## Overview
Detaches from a shared memory message queue and destroys the associated handle, performing necessary cleanup operations to ensure proper resource management and communication with counterpart processes.

## Definition
```c
void shm_mq_detach(shm_mq_handle *mqh)
```

## Detailed Description
This function performs a comprehensive cleanup sequence when detaching from a shared memory message queue. It ensures that any pending data is properly committed, notifies the counterpart process of the detachment, cancels any registered cleanup callbacks, and releases all associated local memory resources.

The detachment process follows a specific sequence: first, any pending send data is committed to ensure data consistency; then the internal detachment mechanism is triggered to notify other processes; any dynamic shared memory (DSM) segment callbacks are cancelled; and finally, local memory buffers and the handle itself are freed.

## Parameters / Member Variables
- `mqh`: Handle to the shared memory message queue to detach from, containing queue pointer, pending data information, and associated resources

## Dependencies
- Functions called/Symbols referenced:
  - [shm_mq_inc_bytes_written](shm_mq_inc_bytes_written.md)
  - [shm_mq_detach_internal](shm_mq_detach_internal.md)
  - [cancel_on_dsm_detach](../c/cancel_on_dsm_detach.md)
  - [shm_mq_detach_callback](shm_mq_detach_callback.md)
  - [pfree](../p/pfree.md)
- Called from (representative examples):
  - [LaunchParallelWorkers](../L/LaunchParallelWorkers.md)
  - [DestroyParallelContext](../D/DestroyParallelContext.md)
  - [HandleParallelMessage](../H/HandleParallelMessage.md)
  - [ExecParallelFinish](../E/ExecParallelFinish.md)
  - [tqueueShutdownReceiver](../t/tqueueShutdownReceiver.md)

## Notes and Other Information
- Ensures any pending send data is committed before detachment to prevent data loss
- Properly notifies counterpart processes to avoid hanging waits
- Handles cleanup of DSM segment callbacks to prevent memory leaks
- Critical for proper resource management in PostgreSQL's parallel processing framework
- Used extensively in parallel query execution, tuple queues, and logical replication
- Must be called to avoid resource leaks when done with a message queue

## Simplified Source

```c
// Simplified version of shm_mq_detach
void shm_mq_detach(shm_mq_handle *mqh) {
    // Commit any pending send data before detaching
    if (mqh->mqh_send_pending > 0) {
        shm_mq_inc_bytes_written(mqh->mqh_queue, mqh->mqh_send_pending);
        mqh->mqh_send_pending = 0;
    }

    // Notify counterparty of detachment
    shm_mq_detach_internal(mqh->mqh_queue);

    // Cancel DSM segment cleanup callback if registered
    if (mqh->mqh_segment) {
        cancel_on_dsm_detach(mqh->mqh_segment,
                            shm_mq_detach_callback,
                            PointerGetDatum(mqh->mqh_queue));
    }

    // Free local memory resources
    if (mqh->mqh_buffer != NULL)
        pfree(mqh->mqh_buffer);
    pfree(mqh);
}
```

Key simplifications made:
- Added explanatory comments for each cleanup phase
- Preserved the sequential cleanup order for data integrity
- Maintained the pending data commit logic
- Kept the DSM callback cancellation and memory cleanup