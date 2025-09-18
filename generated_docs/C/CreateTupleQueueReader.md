# CreateTupleQueueReader

## Location
src/backend/executor/tqueue.c: 139 - 154

## Overview
Creates and initializes a new TupleQueueReader object used for reading tuples from a shared memory message queue in PostgreSQL's parallel execution framework.

## Definition
TupleQueueReader *CreateTupleQueueReader(shm_mq_handle *handle)

## Detailed Description
This function allocates and initializes a TupleQueueReader structure that wraps a shared memory message queue handle. The TupleQueueReader provides an interface for receiving tuples from other PostgreSQL processes through shared memory queues. It's primarily used in parallel query execution where worker processes send tuples back to the main backend process. The function performs zero-initialization of the structure using palloc0() and sets up the queue handle reference.

## Parameters / Member Variables
- `handle`: A pointer to a shared memory message queue handle (shm_mq_handle) that will be used for receiving tuple data from other processes

## Dependencies
- Functions called/Symbols referenced:
  - palloc0 (memory allocation)
  - shm_mq_handle (shared memory queue handle type)
  - TupleQueueReader (struct type)
- Called from (representative examples):
  - ExecParallelCreateReaders

## Notes and Other Information
- The returned TupleQueueReader should be freed using DestroyTupleQueueReader() when no longer needed
- The underlying shm_mq_handle is managed by the caller - this function only stores a reference to it
- Uses palloc0() for zero-initialized allocation, ensuring all fields start with known values
- Part of PostgreSQL's parallel execution infrastructure for inter-process tuple communication