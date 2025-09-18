# CreateTupleQueueDestReceiver

## Location
src/backend/executor/tqueue.c: 119 - 138

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
  - palloc0
  - tqueueReceiveSlot (assigned as receiveSlot callback)
  - tqueueStartupReceiver (assigned as rStartup callback)
  - tqueueShutdownReceiver (assigned as rShutdown callback)
  - tqueueDestroyReceiver (assigned as rDestroy callback)
  - DestTupleQueue (destination type identifier)
- Called from (representative examples):
  - ExecParallelGetReceiver
  - CreateDestReceiver

## Notes and Other Information
- Returns a DestReceiver pointer that can be used with PostgreSQL's executor framework
- Uses palloc0 for zero-initialized memory allocation following PostgreSQL conventions
- Establishes the complete DestReceiver interface by setting all required callback functions
- Critical component for parallel query execution, enabling tuple distribution across worker processes
- The returned receiver manages the lifecycle of shared memory queue communication
- Part of PostgreSQL's pluggable destination receiver architecture