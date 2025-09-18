# TupleQueueReader

## Location
src/backend/executor/tqueue.c: 43 - 53

## Overview
TupleQueueReader is a structure used for reading tuples from shared memory message queues in PostgreSQL parallel query execution, enabling communication between parallel worker processes and the coordinating backend.

## Definition
```c
struct TupleQueueReader
{
    shm_mq_handle *queue;   /* shm_mq to receive from */
};
```

## Detailed Description
TupleQueueReader is a core component of PostgreSQL's parallel query execution infrastructure that facilitates reading tuples from shared memory message queues. This structure provides a simple but effective abstraction for receiving tuples that have been sent by parallel worker processes through shared memory communication channels.

The reader operates as the receiving end of the tuple queue communication system, complementing TQueueDestReceiver which handles the sending side. It's designed to be lightweight and efficient, containing only the essential shared memory queue handle needed to retrieve tuples from parallel workers.

This structure is extensively used in parallel execution nodes like Gather and GatherMerge, which need to collect results from multiple parallel worker processes and merge them into a single result stream.

## Parameters / Member Variables
- `queue`: Handle to the shared memory message queue from which tuples are received from parallel worker processes

## Dependencies
- Functions called/Symbols referenced:
  - [shm_mq_handle](../s/shm_mq_handle.md)
- Called from (representative examples):
  - [ExecParallelCreateReaders](../E/ExecParallelCreateReaders.md)
  - [ExecGather](../E/ExecGather.md)
  - [gather_readnext](../g/gather_readnext.md)
  - [ExecGatherMerge](../E/ExecGatherMerge.md)
  - [gm_readnext_tuple](../g/gm_readnext_tuple.md)
  - [CreateTupleQueueReader](../C/CreateTupleQueueReader.md)
  - [DestroyTupleQueueReader](../D/DestroyTupleQueueReader.md)
  - [TupleQueueReaderNext](TupleQueueReaderNext.md)

## Notes and Other Information
- Located in src/backend/executor/tqueue.c:43-53
- Essential component of PostgreSQL's parallel query execution system
- Used extensively in Gather and GatherMerge execution nodes
- Provides the receiving endpoint for tuple communication in parallel processing
- Works in conjunction with TQueueDestReceiver for complete tuple queue communication
- The structure definition is typedef'd in tqueue.h for external access
- Integrates with PostgreSQL's shared memory message queue infrastructure for efficient inter-process communication