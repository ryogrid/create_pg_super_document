# CreateTupleQueueDestReceiver

## Location
[src/backend/executor/tqueue.c:119-138](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/tqueue.c#L119-L138)

## Overview
Creates and initializes a destination receiver that writes tuples to a shared memory message queue for inter-process communication in parallel query execution.

## Definition
```c
DestReceiver *CreateTupleQueueDestReceiver(shm_mq_handle *handle)
```

## Detailed Description
This function serves as a factory function for creating TQueueDestReceiver objects, which implement the DestReceiver interface for tuple queue destinations. It allocates memory for the receiver structure, initializes all callback function pointers to their respective implementation functions, sets the destination type identifier, and associates the receiver with the provided shared memory queue handle. This receiver is used in PostgreSQL's parallel execution framework to send tuples between parallel worker processes through shared memory queues.

## Parameters / Member Variables
- `handle`: shm_mq_handle pointer representing the shared memory queue connection for sending tuples

## Dependencies
- Functions called/Symbols referenced:
  - [palloc0](../p/palloc0.md)
  - [tqueueReceiveSlot](../t/tqueueReceiveSlot.md) (assigned as receiveSlot callback)
  - [tqueueStartupReceiver](../t/tqueueStartupReceiver.md) (assigned as rStartup callback)
  - [tqueueShutdownReceiver](../t/tqueueShutdownReceiver.md) (assigned as rShutdown callback)
  - [tqueueDestroyReceiver](../t/tqueueDestroyReceiver.md) (assigned as rDestroy callback)
  - DestTupleQueue (destination type identifier)
- Called from (representative examples):
  - [ExecParallelGetReceiver](../E/ExecParallelGetReceiver.md)
  - [CreateDestReceiver](CreateDestReceiver.md)

## Notes and Other Information
- Returns a DestReceiver pointer that can be used with PostgreSQL's executor framework
- Uses palloc0 for zero-initialized memory allocation following PostgreSQL conventions
- Establishes the complete DestReceiver interface by setting all required callback functions
- Critical component for parallel query execution, enabling tuple distribution across worker processes
- The returned receiver manages the lifecycle of shared memory queue communication
- Part of PostgreSQL's pluggable destination receiver architecture

## Simplified Source

```c
// Simplified version of CreateTupleQueueDestReceiver
DestReceiver *CreateTupleQueueDestReceiver(shm_mq_handle *handle) {
    // Allocate and zero-initialize the tuple queue destination receiver
    TQueueDestReceiver *queue_receiver = (TQueueDestReceiver *) palloc0(sizeof(TQueueDestReceiver));

    // Set up the callback functions for tuple queue operations
    queue_receiver->pub.receiveSlot = tqueueReceiveSlot;           // Send each tuple to queue
    queue_receiver->pub.rStartup = tqueueStartupReceiver;          // Initialize queue connection
    queue_receiver->pub.rShutdown = tqueueShutdownReceiver;        // Finalize queue communication
    queue_receiver->pub.rDestroy = tqueueDestroyReceiver;          // Final cleanup

    // Set the destination type to indicate tuple queue operation
    queue_receiver->pub.mydest = DestTupleQueue;

    // Store the shared memory queue handle for inter-process communication
    queue_receiver->queue = handle;

    // Return as base DestReceiver type
    return (DestReceiver *) queue_receiver;
}
```

Key simplifications made:
- Added descriptive variable name for clarity
- Added comments explaining each callback function's purpose
- Clarified the role of the shared memory queue handle for parallel execution
- Explained the inter-process communication aspect
- Focused on core logic: allocate memory, set callbacks, store handle, return receiver