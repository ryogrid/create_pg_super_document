# ExecParallelGetReceiver

## Location
src/backend/executor/execParallel.c: 1220 - 1235

## Overview
Creates a DestReceiver to write tuples produced by a parallel worker to the shared memory queue designated for that purpose.

## Definition
```c
static DestReceiver *ExecParallelGetReceiver(dsm_segment *seg, shm_toc *toc)
```

## Detailed Description
This static function is part of PostgreSQL's parallel query execution infrastructure. It sets up the communication mechanism for a parallel worker to send tuple data back to the leader process. The function locates the appropriate shared memory queue (shm_mq) for the current worker based on the worker number, configures it as a sender, and wraps it in a DestReceiver interface that can be used by the executor to output tuples.

The function calculates the worker-specific queue location by adding an offset based on the worker number multiplied by the queue size, ensuring each worker has its own dedicated communication channel.

## Parameters / Member Variables
- `seg`: Dynamic shared memory segment containing the parallel query infrastructure
- `toc`: Shared memory table of contents used to locate specific data structures within the segment

## Dependencies
- Functions called/Symbols referenced:
  - shm_toc_lookup
  - shm_mq_set_sender
  - shm_mq_attach
  - CreateTupleQueueDestReceiver
- Constants used:
  - PARALLEL_KEY_TUPLE_QUEUE
  - PARALLEL_TUPLE_QUEUE_SIZE
- Global variables:
  - ParallelWorkerNumber
  - MyProc
- Called from:
  - ParallelQueryMain

## Notes and Other Information
- This is a static function, only accessible within execParallel.c
- Each parallel worker gets its own dedicated tuple queue to avoid contention
- The function assumes ParallelWorkerNumber is properly set for the current worker
- The returned DestReceiver should be used as the destination for tuple output during parallel execution
- Part of the broader parallel query execution framework that enables PostgreSQL to distribute query processing across multiple worker processes