# LogicalParallelApplyLoop

## Location
[src/backend/replication/logical/applyparallelworker.c:734-843](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/applyparallelworker.c#L734-L843)

## Overview
LogicalParallelApplyLoop implements the main processing loop for PostgreSQL logical replication parallel apply workers, handling message reception and processing from the leader apply worker via shared memory queues.

## Definition
```c
static void LogicalParallelApplyLoop(shm_mq_handle *mqh)
```

## Detailed Description
This function serves as the core event loop for parallel apply workers in PostgreSQL's logical replication system. It establishes a memory context for processing replication messages, sets up error handling callbacks, and enters an infinite loop to process messages from the leader apply worker.

The function receives messages through shared memory queues, validates message format (expecting messages to start with 'w'), and dispatches them for processing. It handles three main shared memory queue states: successful message receipt (SHM_MQ_SUCCESS), blocking scenarios where no messages are available (SHM_MQ_WOULD_BLOCK), and connection loss (SHM_MQ_DETACHED).

When no messages are immediately available, the worker processes any spooled messages from previous operations and waits on a latch for new work. The function maintains proper memory context management, resetting the message context after each message to prevent memory leaks.

## Parameters / Member Variables
- `mqh`: Handle to the shared memory queue for receiving messages from the leader apply worker

## Dependencies
- Functions called/Symbols referenced:
  - AllocSetContextCreate
  - [ProcessParallelApplyInterrupts](../P/ProcessParallelApplyInterrupts.md)
  - [shm_mq_receive](../s/shm_mq_receive.md)
  - [initReadOnlyStringInfo](../i/initReadOnlyStringInfo.md)
  - [pq_getmsgbyte](../p/pq_getmsgbyte.md)
  - [apply_dispatch](../a/apply_dispatch.md)
  - [pa_process_spooled_messages_if_required](../p/pa_process_spooled_messages_if_required.md)
  - [WaitLatch](../W/WaitLatch.md)
  - [ResetLatch](../R/ResetLatch.md)
  - [MemoryContextReset](../M/MemoryContextReset.md)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)
- Called from (representative examples):
  - [ParallelApplyWorkerMain](../P/ParallelApplyWorkerMain.md)

## Notes and Other Information
- This is a static function, accessible only within applyparallelworker.c
- The function expects all messages to start with 'w' character, representing work messages from the leader
- Statistics fields from leader messages are intentionally ignored to avoid unnecessary processing overhead
- Uses a dedicated ApplyMessageContext for memory management during message processing
- Implements graceful error handling with apply_error_callback context
- Part of PostgreSQL's parallel logical replication architecture for improved replication throughput