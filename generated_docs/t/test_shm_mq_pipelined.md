# test_shm_mq_pipelined

## Location
[src/test/modules/test_shm_mq/test.c:132-257](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/test_shm_mq/test.c#L132-L257)

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
  - [test_shm_mq_setup](test_shm_mq_setup.md): Sets up the dynamic shared memory segment and background workers
  - [shm_mq_send](../s/shm_mq_send.md): Non-blocking send operation through shared memory queues
  - [shm_mq_receive](../s/shm_mq_receive.md): Non-blocking receive operation from shared memory queues
  - [verify_message](../v/verify_message.md): Validates message integrity (optional, controlled by verify parameter)
  - [WaitEventExtensionNew](../W/WaitEventExtensionNew.md): Creates custom wait event for message queue operations
  - [WaitLatch](../W/WaitLatch.md): Efficiently waits for latch signals indicating queue activity
  - [ResetLatch](../R/ResetLatch.md): Resets the latch after being signaled
  - [dsm_detach](../d/dsm_detach.md): Cleans up the dynamic shared memory segment
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

## Simplified Source

```c
Datum
test_shm_mq_pipelined(PG_FUNCTION_ARGS)
{
    // Extract parameters
    int64 queue_size = PG_GETARG_INT64(0);
    text *message = PG_GETARG_TEXT_PP(1);
    char *message_contents = VARDATA_ANY(message);
    int message_size = VARSIZE_ANY_EXHDR(message);
    int32 loop_count = PG_GETARG_INT32(2);
    int32 nworkers = PG_GETARG_INT32(3);
    bool verify = PG_GETARG_BOOL(4);

    int32 send_count = 0;
    int32 receive_count = 0;
    dsm_segment *seg;
    shm_mq_handle *outqh;
    shm_mq_handle *inqh;
    shm_mq_result res;
    Size len;
    void *data;

    // Validate parameters
    if (loop_count < 0)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("repeat count size must be an integer value greater than or equal to zero")));

    if (nworkers < 0)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("number of workers must be an integer value greater than or equal to zero")));

    // Set up shared memory segment and message queues
    test_shm_mq_setup(queue_size, nworkers, &seg, &outqh, &inqh);

    // Pipelined send/receive loop
    for (;;) {
        bool wait = true;

        // Try to send messages (non-blocking)
        if (send_count < loop_count) {
            res = shm_mq_send(outqh, message_size, message_contents, true, true);
            if (res == SHM_MQ_SUCCESS) {
                ++send_count;
                wait = false;
            } else if (res == SHM_MQ_DETACHED) {
                ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                        errmsg("could not send message")));
            }
        }

        // Try to receive messages (non-blocking)
        if (receive_count < loop_count) {
            res = shm_mq_receive(inqh, &len, &data, true);
            if (res == SHM_MQ_SUCCESS) {
                ++receive_count;
                if (verify)
                    verify_message(message_size, message_contents, len, data);
                wait = false;
            } else if (res == SHM_MQ_DETACHED) {
                ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                        errmsg("could not receive message")));
            }
        } else {
            // Finished receiving - verify send/receive counts match
            if (send_count != receive_count)
                ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
                        errmsg("message sent %d times, but received %d times",
                               send_count, receive_count)));
            break;
        }

        // Wait for queue activity if no progress was made
        if (wait) {
            if (we_message_queue == 0)
                we_message_queue = WaitEventExtensionNew("TestShmMqMessageQueue");

            (void) WaitLatch(MyLatch, WL_LATCH_SET | WL_EXIT_ON_PM_DEATH, 0,
                           we_message_queue);
            ResetLatch(MyLatch);
            CHECK_FOR_INTERRUPTS();
        }
    }

    // Clean up
    dsm_detach(seg);

    PG_RETURN_VOID();
}
```