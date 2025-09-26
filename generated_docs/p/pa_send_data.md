# pa_send_data

## Location
[src/backend/replication/logical/applyparallelworker.c:1146-1167](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/applyparallelworker.c#L1146-L1167)

## Overview
Sends data to a specified parallel apply worker via shared-memory queue in PostgreSQL's logical replication system, with timeout and retry logic to prevent blocking.

## Definition

```c
bool
pa_send_data(ParallelApplyWorkerInfo *winfo, Size nbytes, const void *data)
```
## Detailed Description
The  function is a core component of PostgreSQL's parallel logical replication system that handles inter-process communication between the main apply worker and parallel apply workers. It attempts to send data via shared-memory queues using a non-blocking approach with timeout and retry mechanisms.

The function implements a retry loop with a timeout mechanism to prevent indefinite blocking. It uses a 1-second retry interval and a total timeout of approximately 9 seconds. If the shared-memory queue is full (SHM_MQ_WOULD_BLOCK), the function waits on a latch before retrying. This design prevents deadlocks and ensures the main apply process doesn't get stuck waiting for parallel workers.

The function includes several safety checks:
- Ensures it's not called within a transaction state (Assert(!IsTransactionState()))
- Verifies that serialization mode is not enabled for the worker
- Skips sending data in 'immediate' debug mode (used for testing)

If the parallel worker becomes detached (SHM_MQ_DETACHED), the function raises an error. On successful transmission (SHM_MQ_SUCCESS), it returns true. If the timeout is exceeded, it returns false, allowing the caller to handle the failure appropriately.

## Parameters / Member Variables
- : ParallelApplyWorkerInfo pointer containing worker information including the shared-memory queue handle (mq_handle) and serialization state
- : Size of the data to be sent in bytes  
- : Pointer to the data buffer to be transmitted to the parallel worker

## Dependencies
- Functions called/Symbols referenced:
  -  - Checks if currently in a transaction state
  -  - Sends data via shared-memory queue
  -  - Waits on latch with timeout for retry logic
  -  - Resets the latch after being signaled
  -  - Processes pending interrupts
  -  - Gets current timestamp for timeout calculation
  -  - Checks if timeout has been exceeded
  -  - Reports errors when queue becomes detached
- Called from (representative examples):
  -  - Handles streaming transaction data
  -  - Processes stream prepare messages
  -  - Handles stream start events
  -  - Processes stream stop events
  -  - Handles stream abort operations
  -  - Processes stream commit events

## Notes and Other Information
- The function uses compile-time constants SHM_SEND_RETRY_INTERVAL_MS (1000ms) and SHM_SEND_TIMEOUT_MS (9000ms) for timing control
- Returns false in DEBUG_LOGICAL_REP_STREAMING_IMMEDIATE mode for testing purposes
- The timeout mechanism prevents indefinite blocking while allowing sufficient time for normal message transmission
- Part of PostgreSQL's parallel logical replication infrastructure located in applyparallelworker.c:1146-1202
- Uses non-blocking shared-memory queue operations to maintain system responsiveness
- Critical for preventing deadlocks in multi-worker logical replication scenarios