# HandleParallelApplyMessages

## Location
[src/backend/replication/logical/applyparallelworker.c:1063-1145](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/applyparallelworker.c#L1063-L1145)

## Overview
Processes queued protocol messages from all parallel apply workers in logical replication, handling message reception from shared memory queues and dispatching them for individual processing.

## Definition
```c
void HandleParallelApplyMessages(void)
```

## Detailed Description
HandleParallelApplyMessages serves as the main message dispatcher for parallel apply workers in PostgreSQL logical replication. It iterates through the pool of parallel apply workers, checking each workers error message queue for pending messages. The function operates within interrupt-safe boundaries and uses a dedicated memory context to prevent memory leaks. For each worker, it attempts to receive messages from the shared memory queue non-blockingly. Successfully received messages are wrapped in StringInfo and passed to HandleParallelApplyMessage for individual processing. The function handles three possible outcomes from message reception: successful reception, no messages available, or connection loss to the worker.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - HOLD_INTERRUPTS
  - AllocSetContextCreate
  - ALLOCSET_DEFAULT_SIZES
  - [MemoryContextReset](../M/MemoryContextReset.md)
  - [shm_mq_result](../s/shm_mq_result.md)
  - [ParallelApplyWorkerInfo](../P/ParallelApplyWorkerInfo.md)
  - [shm_mq_receive](../s/shm_mq_receive.md)
  - SHM_MQ_WOULD_BLOCK
  - SHM_MQ_SUCCESS
  - [appendBinaryStringInfo](../a/appendBinaryStringInfo.md)
  - [HandleParallelApplyMessage](HandleParallelApplyMessage.md)
  - RESUME_INTERRUPTS

- Called from (representative examples):
  - [ProcessInterrupts](../P/ProcessInterrupts.md)

## Notes and Other Information
- Uses interrupt blocking (HOLD_INTERRUPTS/RESUME_INTERRUPTS) to prevent recursive calls since it can be invoked from ProcessInterrupts()
- Maintains a static memory context "HandleParallelApplyMessages" that is reset on each call to prevent memory leaks
- Sets ParallelApplyMessagePending to false at the start to indicate message processing is underway
- Skips workers whose error_mq_handle is NULL, indicating they have been detached
- Uses non-blocking message reception (shm_mq_receive with nowait=true) to avoid hanging
- Handles connection loss as a fatal error condition
- Memory management is careful to switch contexts and clean up properly

## Simplified Source

```c
// Simplified version of HandleParallelApplyMessages
void HandleParallelApplyMessages(void) {
    ListCell *lc;
    MemoryContext oldcontext;
    static MemoryContext hpam_context = NULL;

    // Block interrupts to prevent recursive calls
    HOLD_INTERRUPTS();

    // Create or reset private memory context
    if (!hpam_context) {
        hpam_context = AllocSetContextCreate(TopMemoryContext,
                                           "HandleParallelApplyMessages",
                                           ALLOCSET_DEFAULT_SIZES);
    } else {
        MemoryContextReset(hpam_context);
    }

    oldcontext = MemoryContextSwitchTo(hpam_context);
    ParallelApplyMessagePending = false;

    // Process messages from each parallel worker
    foreach(lc, ParallelApplyWorkerPool) {
        shm_mq_result res;
        Size nbytes;
        void *data;
        ParallelApplyWorkerInfo *winfo = (ParallelApplyWorkerInfo *) lfirst(lc);

        // Skip detached workers
        if (!winfo->error_mq_handle) {
            continue;
        }

        // Try to receive message non-blockingly
        res = shm_mq_receive(winfo->error_mq_handle, &nbytes, &data, true);

        if (res == SHM_MQ_WOULD_BLOCK) {
            continue;
        } else if (res == SHM_MQ_SUCCESS) {
            // Process received message
            StringInfoData msg;
            initStringInfo(&msg);
            appendBinaryStringInfo(&msg, data, nbytes);
            HandleParallelApplyMessage(&msg);
            pfree(msg.data);
        } else {
            // Connection lost
            ereport(ERROR,
                   (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                    errmsg("lost connection to the logical replication parallel apply worker")));
        }
    }

    // Clean up and restore context
    MemoryContextSwitchTo(oldcontext);
    MemoryContextReset(hpam_context);
    RESUME_INTERRUPTS();
}
```

Key simplifications made:
- Removed detailed comments about recursive calls and memory safety
- Grouped memory context operations together
- Simplified worker processing loop comments
- Maintained essential interrupt protection and error handling
- Preserved all critical functionality and cleanup operations