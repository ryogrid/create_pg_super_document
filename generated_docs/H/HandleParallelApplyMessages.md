# HandleParallelApplyMessages

## Location
src/backend/replication/logical/applyparallelworker.c: 1063 - 1145

## Overview
Processes queued protocol messages from all parallel apply workers in logical replication, handling message reception from shared memory queues and dispatching them for individual processing.

## Definition
```c
void HandleParallelApplyMessages(void)
```

## Detailed Description
HandleParallelApplyMessages serves as the main message dispatcher for parallel apply workers in PostgreSQL logical replication. It iterates through the pool of parallel apply workers, checking each workers error message queue for pending messages. The function operates within interrupt-safe boundaries and uses a dedicated memory context to prevent memory leaks. For each worker, it attempts to receive messages from the shared memory queue non-blockingly. Successfully received messages are wrapped in StringInfo and passed to HandleParallelApplyMessage for individual processing. The function handles three possible outcomes from message reception: successful reception, no messages available, or connection loss to the worker.

## Parameters / Member Variables
- No parameters (void function)

## Dependencies
- Functions called/Symbols referenced:
  - HOLD_INTERRUPTS
  - AllocSetContextCreate
  - ALLOCSET_DEFAULT_SIZES
  - MemoryContextReset
  - shm_mq_result
  - ParallelApplyWorkerInfo
  - shm_mq_receive
  - SHM_MQ_WOULD_BLOCK
  - SHM_MQ_SUCCESS
  - appendBinaryStringInfo
  - HandleParallelApplyMessage
  - RESUME_INTERRUPTS

- Called from (representative examples):
  - ProcessInterrupts

## Notes and Other Information
- Uses interrupt blocking (HOLD_INTERRUPTS/RESUME_INTERRUPTS) to prevent recursive calls since it can be invoked from ProcessInterrupts()
- Maintains a static memory context "HandleParallelApplyMessages" that is reset on each call to prevent memory leaks
- Sets ParallelApplyMessagePending to false at the start to indicate message processing is underway
- Skips workers whose error_mq_handle is NULL, indicating they have been detached
- Uses non-blocking message reception (shm_mq_receive with nowait=true) to avoid hanging
- Handles connection loss as a fatal error condition
- Memory management is careful to switch contexts and clean up properly