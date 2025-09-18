# test_shm_mq_pipelined

## Location
src/test/modules/test_shm_mq/test.c: 132 - 257

## Overview
A pipelined test function that validates PostgreSQL's shared memory message queue infrastructure by sending multiple copies of a message concurrently through a ring of background processes using non-blocking operations.

## Definition
```c
Datum test_shm_mq_pipelined(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements an advanced pipelined test of the shared memory message queue (shm_mq) infrastructure. Unlike the basic test_shm_mq function, this version uses non-blocking operations to send multiple copies of a message through the queue ring concurrently. 

Key features:
1. Uses non-blocking send/receive operations to avoid deadlocks
2. Can handle scenarios where message queues fill up by interleaving send and receive operations
3. Supports sending data to itself (0 workers allowed due to non-blocking design)
4. Provides optional message verification for performance testing
5. Uses PostgreSQL's latch mechanism to efficiently wait for queue availability

The function maintains separate counters for sent and received messages, ensuring all sent messages are eventually received. It uses a wait/latch mechanism when no progress can be made, allowing efficient coordination with background worker processes.

## Parameters / Member Variables
- `queue_size` (int64): Size of each message queue in the ring
- `message` (text*): The message to send through the queue ring multiple times
- `loop_count` (int32): Number of copies of the message to send/receive
- `nworkers` (int32): Number of background worker processes (0 or more allowed)
- `verify` (bool): Whether to verify message integrity on each receive (optional for performance)

## Dependencies
- Functions called/Symbols referenced:
  - test_shm_mq_setup: Sets up the dynamic shared memory segment and background workers
  - shm_mq_send: Non-blocking send operation through shared memory queues
  - shm_mq_receive: Non-blocking receive operation from shared memory queues
  - verify_message: Validates message integrity (optional, controlled by verify parameter)
  - WaitEventExtensionNew: Creates custom wait event for message queue operations
  - WaitLatch: Efficiently waits for latch signals indicating queue activity
  - ResetLatch: Resets the latch after being signaled
  - dsm_detach: Cleans up the dynamic shared memory segment
  - CHECK_FOR_INTERRUPTS: Checks for query cancellation during wait periods
- Called from (representative examples):
  - No direct callers found (likely called via SQL function interface)

## Notes and Other Information
- Uses non-blocking operations, allowing 0 workers (can send to itself)
- More complex than basic test due to pipelined operation and potential queue saturation
- Uses PostgreSQL's latch mechanism for efficient waiting when queues are full/empty
- Message verification is optional to allow performance testing without integrity overhead
- Handles SHM_MQ_WOULD_BLOCK results by maintaining state and retrying operations
- Part of PostgreSQL's regression test suite for advanced shared memory queue scenarios
- Demonstrates proper use of non-blocking shm_mq interfaces in high-throughput scenarios
- Located in the test_shm_mq extension module alongside the basic test function