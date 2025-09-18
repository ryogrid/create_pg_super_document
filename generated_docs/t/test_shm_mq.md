# test_shm_mq

## Location
[src/test/modules/test_shm_mq/test.c:43-131](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/test_shm_mq/test.c#L43-L131)

## Overview
A test function that validates PostgreSQL's shared memory message queue infrastructure by creating a ring of message queues through background processes and verifying message transmission integrity.

## Definition


## Detailed Description
This function implements a comprehensive test of the shared memory message queue (shm_mq) infrastructure. It creates a ring topology where messages pass through one or more background worker processes before returning to the sender. The test validates that:

1. Messages can be successfully sent and received through the shared memory queue system
2. The message content remains intact after passing through multiple workers
3. The infrastructure handles blocking send/receive operations correctly

The function sets up a dynamic shared memory segment with background workers, sends an initial message, then enters a loop where it receives messages and forwards them back out. After the specified number of iterations, it verifies the final received message matches the original.

## Parameters / Member Variables
-  (int64): Size of each message queue in the ring
-  (text*): The initial message to send through the queue ring  
-  (int32): Number of times to pass the message around the ring
-  (int32): Number of background worker processes to create in the ring

## Dependencies
- Functions called/Symbols referenced:
  - [test_shm_mq_setup](test_shm_mq_setup.md): Sets up the dynamic shared memory segment and background workers
  - [shm_mq_send](../s/shm_mq_send.md): Sends messages through shared memory queues
  - [shm_mq_receive](../s/shm_mq_receive.md): Receives messages from shared memory queues
  - [verify_message](../v/verify_message.md): Validates message integrity by comparing original and final messages
  - [dsm_detach](../d/dsm_detach.md): Cleans up the dynamic shared memory segment
  - PG_GETARG_INT64, PG_GETARG_TEXT_PP, PG_GETARG_INT32: PostgreSQL argument extraction macros
  - PG_RETURN_VOID: PostgreSQL return macro
- Called from (representative examples):
  - No direct callers found (likely called via SQL function interface)

## Notes and Other Information
- This is a blocking test that cannot send data to itself, requiring at least 1 worker process
- Located in the test_shm_mq extension module for testing shared memory queue functionality
- Validates parameter bounds (non-negative loop count, positive worker count)
- Uses PostgreSQL's dynamic shared memory (DSM) infrastructure
- Part of PostgreSQL's regression test suite for shared memory message queues
- The function signature follows PostgreSQL's version-1 calling convention for SQL-callable functions