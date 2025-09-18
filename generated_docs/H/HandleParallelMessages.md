# HandleParallelMessages

## Location
src/backend/access/transam/parallel.c: 1044 - 1132

## Overview
Processes all queued protocol messages received from parallel workers, handling communication between the main backend and its parallel worker processes.

## Definition


## Detailed Description
This function is the main message processing routine for parallel worker communication. It is called from ProcessInterrupts() when the ParallelMessagePending flag is set (typically by HandleParallelMessageInterrupt()). The function iterates through all active parallel contexts and processes messages from each worker's error queue.

The function implements several important safety measures:
1. **Interrupt blocking**: Uses HOLD_INTERRUPTS()/RESUME_INTERRUPTS() to prevent recursive calls during message processing
2. **Memory management**: Uses a dedicated memory context that is reset on each call to prevent memory leaks
3. **Non-blocking reads**: Reads messages without blocking, stopping when no more messages are available

For each parallel context, it processes messages from all launched workers by reading from their shared memory message queues (error_mqh). Messages are read using shm_mq_receive() and then passed to HandleParallelMessage() for specific message type processing.

## Parameters / Member Variables
This function takes no parameters.

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
  - initStringInfo
  - appendBinaryStringInfo
  - [HandleParallelMessage](HandleParallelMessage.md)
  - [pfree](../p/pfree.md)
  - ereport

- Called from (representative examples):
  - ProcessInterrupts
  - IsParallelWorker (referenced in header)

## Notes and Other Information
- This function is designed to be interrupt-safe and memory-leak-free
- It processes messages from all parallel workers in all active parallel contexts
- The function uses shared memory message queues for inter-process communication
- Error handling includes reporting lost connections to parallel workers
- The dedicated memory context ('HandleParallelMessages') ensures clean memory management
- Messages are processed non-blocking to avoid hanging the main process