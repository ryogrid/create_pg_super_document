# tqueueDestroyReceiver

## Location
[src/backend/executor/tqueue.c:105-118](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/tqueue.c#L105-L118)

## Overview
Destroys a tuple queue destination receiver by detaching from the shared memory queue and freeing the allocated memory.

## Definition
```c
static void tqueueDestroyReceiver(DestReceiver *self)
```

## Detailed Description
This function serves as the destruction callback for TQueueDestReceiver objects, providing final cleanup when the receiver is no longer needed. It implements a defensive programming approach by ensuring the shared memory queue is detached even if shutdown was already called, then frees the memory allocated for the receiver structure. This function is part of PostgreSQL's resource management lifecycle for destination receivers.

## Parameters / Member Variables
- `self`: DestReceiver pointer that is cast to TQueueDestReceiver for cleanup operations

## Dependencies
- Functions called/Symbols referenced:
  - [TQueueDestReceiver](../T/TQueueDestReceiver.md) (cast type)
  - [shm_mq_detach](../s/shm_mq_detach.md)
  - [pfree](../p/pfree.md)
- Called from (representative examples):
  - [CreateTupleQueueDestReceiver](../C/CreateTupleQueueDestReceiver.md) (assigned as rDestroy callback)

## Notes and Other Information
- Implements defensive programming by checking and detaching from queue even if already detached
- Ensures complete memory cleanup by calling pfree on the receiver structure
- Part of the DestReceiver destruction lifecycle in PostgreSQL's executor framework
- Prevents memory leaks and ensures proper shared memory queue reference counting
- Typically called after shutdown but provides additional safety for queue detachment