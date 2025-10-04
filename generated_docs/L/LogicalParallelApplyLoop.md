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

## Simplified Source

```c
static void LogicalParallelApplyLoop(shm_mq_handle *mqh) {
    // Initialize memory context for message processing
    ApplyMessageContext = AllocSetContextCreate(ApplyContext, "ApplyMessageContext", ALLOCSET_DEFAULT_SIZES);

    // Set up error callback
    ErrorContextCallback errcallback;
    errcallback.callback = apply_error_callback;
    errcallback.previous = error_context_stack;
    error_context_stack = &errcallback;

    // Main processing loop
    for (;;) {
        void *data;
        Size len;

        ProcessParallelApplyInterrupts();
        MemoryContextSwitchTo(ApplyMessageContext);

        // Try to receive message from shared memory queue
        shm_mq_result shmq_res = shm_mq_receive(mqh, &len, &data, true);

        if (shmq_res == SHM_MQ_SUCCESS) {
            // Process received message
            StringInfoData s;
            initReadOnlyStringInfo(&s, data, len);

            // Verify message type (must be 'w' for work)
            int c = pq_getmsgbyte(&s);
            if (c != 'w')
                elog(ERROR, "unexpected message \"%c\"", c);

            // Skip statistics fields and dispatch message
            s.cursor += SIZE_STATS_MESSAGE;
            apply_dispatch(&s);

        } else if (shmq_res == SHM_MQ_WOULD_BLOCK) {
            // No immediate work - process spooled messages or wait
            if (!pa_process_spooled_messages_if_required()) {
                int rc = WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH, 1000L, WAIT_EVENT_LOGICAL_PARALLEL_APPLY_MAIN);
                if (rc & WL_LATCH_SET)
                    ResetLatch(MyLatch);
            }
        } else {
            // Connection lost
            ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                           errmsg("lost connection to the logical replication apply worker")));
        }

        // Clean up memory after processing
        MemoryContextReset(ApplyMessageContext);
    }

    // Cleanup error context
    error_context_stack = errcallback.previous;
}
```