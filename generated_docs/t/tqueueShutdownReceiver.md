# tqueueShutdownReceiver

## Location
src/backend/executor/tqueue.c: 92 - 104

## Overview
Cleans up shared memory queue resources at the end of an executor run by detaching from the queue and resetting the connection.

## Definition
```c
static void tqueueShutdownReceiver(DestReceiver *self)
```

## Detailed Description
This function serves as the shutdown callback for TQueueDestReceiver objects, performing cleanup operations when query execution completes. It handles the proper detachment from the shared memory message queue to ensure clean resource deallocation and prevents resource leaks. The function follows PostgreSQL's resource management patterns by checking for null pointers and setting the queue reference to NULL after detachment.

## Parameters / Member Variables
- `self`: DestReceiver pointer that is cast to TQueueDestReceiver to access queue information

## Dependencies
- Functions called/Symbols referenced:
  - [TQueueDestReceiver](../T/TQueueDestReceiver.md) (cast type)
  - [shm_mq_detach](../s/shm_mq_detach.md)
- Called from (representative examples):
  - [CreateTupleQueueDestReceiver](../C/CreateTupleQueueDestReceiver.md) (assigned as rShutdown callback)

## Notes and Other Information
- Ensures proper cleanup by detaching from shared memory queues to prevent resource leaks
- Includes null pointer check before attempting to detach from the queue
- Sets queue pointer to NULL after detachment following PostgreSQL coding conventions
- Part of the DestReceiver cleanup lifecycle for parallel execution infrastructure
- Critical for maintaining proper shared memory queue reference counts and cleanup