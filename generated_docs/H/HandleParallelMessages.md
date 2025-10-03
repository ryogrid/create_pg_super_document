# HandleParallelMessages

## Location
[src/backend/access/transam/parallel.c:1044-1132](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/parallel.c#L1044-L1132)

## Overview
Processes all queued protocol messages received from parallel workers, handling communication between the main backend and its parallel worker processes.

## Definition

```c
void
HandleParallelMessages(void)
```
## Detailed Description
This function is the main message processing routine for parallel worker communication. It is called from ProcessInterrupts() when the ParallelMessagePending flag is set (typically by HandleParallelMessageInterrupt()). The function iterates through all active parallel contexts and processes messages from each worker's error queue.

The function implements several important safety measures:
1. **Interrupt blocking**: Uses HOLD_INTERRUPTS()/RESUME_INTERRUPTS() to prevent recursive calls during message processing
2. **Memory management**: Uses a dedicated memory context that is reset on each call to prevent memory leaks
3. **Non-blocking reads**: Reads messages without blocking, stopping when no more messages are available

For each parallel context, it processes messages from all launched workers by reading from their shared memory message queues (error_mqh). Messages are read using shm_mq_receive() and then passed to HandleParallelMessage() for specific message type processing.

## Parameters / Member Variables

## Dependencies
- Functions called/Symbols referenced:
  - HOLD_INTERRUPTS
  - RESUME_INTERRUPTS
  - AllocSetContextCreate
  - [MemoryContextReset](../M/MemoryContextReset.md)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)
  - dlist_foreach
  - dlist_container
  - [shm_mq_receive](../s/shm_mq_receive.md)
  - [initStringInfo](../i/initStringInfo.md)
  - [appendBinaryStringInfo](../a/appendBinaryStringInfo.md)
  - [HandleParallelMessage](HandleParallelMessage.md)
  - [pfree](../p/pfree.md)
  - ereport

- Called from (representative examples):
  - [ProcessInterrupts](../P/ProcessInterrupts.md)
  - IsParallelWorker (referenced in header)

## Notes and Other Information
- This function is designed to be interrupt-safe and memory-leak-free
- It processes messages from all parallel workers in all active parallel contexts
- The function uses shared memory message queues for inter-process communication
- Error handling includes reporting lost connections to parallel workers
- The dedicated memory context ('HandleParallelMessages') ensures clean memory management
- Messages are processed non-blocking to avoid hanging the main process

## Simplified Source

```c
// Simplified version of HandleParallelMessages
void HandleParallelMessages(void) {
    // Block interrupts to prevent recursive calls
    HOLD_INTERRUPTS();

    // Set up dedicated memory context for safe memory management
    if (hpm_context == NULL) {
        hpm_context = AllocSetContextCreate(TopMemoryContext, "HandleParallelMessages", ALLOCSET_DEFAULT_SIZES);
    } else {
        MemoryContextReset(hpm_context);
    }
    oldcontext = MemoryContextSwitchTo(hpm_context);

    // Clear the pending message flag
    ParallelMessagePending = false;

    // Process messages from all parallel contexts
    dlist_foreach(iter, &pcxt_list) {
        ParallelContext *pcxt = dlist_container(ParallelContext, node, iter.cur);

        if (pcxt->worker == NULL) continue;

        // Check each worker for messages
        for (int i = 0; i < pcxt->nworkers_launched; ++i) {
            // Read all available messages from this worker
            while (pcxt->worker[i].error_mqh != NULL) {
                res = shm_mq_receive(pcxt->worker[i].error_mqh, &nbytes, &data, true);

                if (res == SHM_MQ_WOULD_BLOCK) {
                    break;  // No more messages available
                } else if (res == SHM_MQ_SUCCESS) {
                    // Process the received message
                    StringInfoData msg;
                    initStringInfo(&msg);
                    appendBinaryStringInfo(&msg, data, nbytes);
                    HandleParallelMessage(pcxt, i, &msg);
                    pfree(msg.data);
                } else {
                    // Handle connection error
                    ereport(ERROR, (errmsg("lost connection to parallel worker")));
                }
            }
        }
    }

    // Restore memory context and cleanup
    MemoryContextSwitchTo(oldcontext);
    MemoryContextReset(hpm_context);
    RESUME_INTERRUPTS();
}
```

Key simplifications made:
- Removed detailed comments for brevity while preserving essential logic
- Consolidated variable declarations
- Simplified error handling structure
- Focused on the main message processing flow
- Maintained all critical functionality and safety measures