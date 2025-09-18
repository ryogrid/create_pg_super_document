# mq_is_send_pending

## Location
[src/backend/libpq/pqmq.c:106-117](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/pqmq.c#L106-L117)

## Overview
A function that always returns false, indicating that there are never any pending send operations in the shared memory message queue communication system.

## Definition
```c
static bool mq_is_send_pending(void)
```

## Detailed Description
The mq_is_send_pending function is part of the PqCommMqMethods structure that implements the PQcommMethods interface for shared memory message queue (shm_mq) communication in PostgreSQL. This function is designed to check whether there are any pending send operations that have not yet been completed.

In the context of shared memory message queues, messages are delivered immediately and synchronously, so there is never a situation where a send operation would be pending. Unlike socket-based communication where data might be buffered in the kernel or network stack and require checking for completion status, shared memory queues either succeed immediately or fail immediately.

The function consistently returns false (0) to indicate that no send operations are ever pending in the shared memory message queue implementation.

## Parameters / Member Variables
- None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - None (no function calls)
- Called from (representative examples):
  - Accessed through PqCommMqMethods.is_send_pending function pointer
  - Used in shared memory message queue communication contexts to check for pending operations

## Notes and Other Information
- This function is part of the shared memory message queue communication implementation (pqmq.c)
- It provides the is_send_pending method for the PQcommMethods interface
- The function always returns false (0) as shared memory queues have no concept of pending sends
- Located in src/backend/libpq/pqmq.c at lines 106-110
- This is a static function, not directly callable from outside pqmq.c
- The return type is bool but the implementation returns 0 (which converts to false)