# TQueueDestReceiver

## Location
src/backend/executor/tqueue.c: 30 - 34

## Overview
TQueueDestReceiver is a specialized DestReceiver structure used for sending tuples through a shared memory message queue in PostgreSQL parallel query execution.

## Definition
```c
typedef struct TQueueDestReceiver
{
    DestReceiver pub;        /* public fields */
    shm_mq_handle *queue;   /* shm_mq to send to */
} TQueueDestReceiver;
```

## Detailed Description
TQueueDestReceiver is a concrete implementation of the DestReceiver interface specifically designed for tuple queue operations in PostgreSQL. It extends the base DestReceiver structure with a shared memory message queue handle, enabling efficient inter-process communication for parallel query execution. This structure serves as the private implementation details for destination receivers that need to send tuples through shared memory queues between parallel worker processes and the main backend process.

The structure encapsulates both the standard DestReceiver interface (`pub` field) and the specific queue communication mechanism (`queue` field), providing a clean abstraction for tuple transmission in parallel execution contexts.

## Parameters / Member Variables
- `pub`: Base DestReceiver structure containing the public interface fields and function pointers
- `queue`: Handle to the shared memory message queue used for sending tuples to the destination process

## Dependencies
- Functions called/Symbols referenced:
  - DestReceiver
  - [shm_mq_handle](../s/shm_mq_handle.md)
- Called from (representative examples):
  - [tqueueReceiveSlot](../t/tqueueReceiveSlot.md)
  - [tqueueShutdownReceiver](../t/tqueueShutdownReceiver.md)
  - [tqueueDestroyReceiver](../t/tqueueDestroyReceiver.md)
  - [CreateTupleQueueDestReceiver](../C/CreateTupleQueueDestReceiver.md)

## Notes and Other Information
- Located in src/backend/executor/tqueue.c:30-34
- This structure is part of PostgreSQL's parallel query execution infrastructure
- The queue field provides the communication channel for sending tuples between parallel processes
- Used internally by the tuple queue destination receiver implementation
- Integrates with PostgreSQL's shared memory message queue system for efficient parallel processing