# tqueueReceiveSlot

## Location
[src/backend/executor/tqueue.c:54-82](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/tqueue.c#L54-L82)

## Overview
Receives a tuple from a query and sends it to a designated shared memory message queue (shm_mq), serving as a tuple destination receiver callback function.

## Definition
```c
static bool tqueueReceiveSlot(TupleTableSlot *slot, DestReceiver *self)
```

## Detailed Description
This function is a callback implementation of the DestReceiver interface specifically for tuple queue destinations. It handles the process of receiving tuples from query execution and forwarding them through shared memory message queues for inter-process communication. The function converts the tuple into a minimal tuple format for efficient transmission, sends it via the shared memory queue, and handles potential transmission errors or queue detachment scenarios.

## Parameters / Member Variables
- `slot`: TupleTableSlot containing the tuple data to be transmitted
- `self`: DestReceiver pointer cast to TQueueDestReceiver containing queue information

## Dependencies
- Functions called/Symbols referenced:
  - [ExecFetchSlotMinimalTuple](../E/ExecFetchSlotMinimalTuple.md)
  - [shm_mq_send](../s/shm_mq_send.md)
  - [pfree](../p/pfree.md)
  - ereport
  - SHM_MQ_DETACHED
  - SHM_MQ_SUCCESS
- Called from (representative examples):
  - [CreateTupleQueueDestReceiver](../C/CreateTupleQueueDestReceiver.md) (assigned as receiveSlot callback)

## Notes and Other Information
- Returns true on successful transmission, false if the shared memory queue has been detached
- Handles memory management by freeing minimal tuples when should_free flag is set
- Provides error reporting for queue transmission failures with appropriate error codes
- Part of PostgreSQL's parallel execution infrastructure for inter-worker communication