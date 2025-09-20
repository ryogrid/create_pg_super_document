# ParallelWorkerContext

## Location
[src/include/access/parallel.h:50-54](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/parallel.h#L50-L54)

## Overview
A lightweight structure used by parallel worker processes to access shared memory segments and their table of contents during parallel execution.

## Definition

```c
typedef struct ParallelWorkerContext
{
	dsm_segment *seg;
	shm_toc    *toc;
} ParallelWorkerContext;
```
## Detailed Description
ParallelWorkerContext is a simple but essential structure used within parallel worker processes to maintain access to the shared memory infrastructure. Unlike ParallelContext which is used by the leader process to manage multiple workers, ParallelWorkerContext provides each individual worker process with the minimal set of handles needed to access shared data structures. This structure serves as the worker's window into the shared execution environment, allowing workers to locate and access shared query plans, tuple queues, hash tables, and other parallel execution state.

## Parameters / Member Variables
- `*seg`: Pointer to the dynamic shared memory segment that contains all shared parallel execution data
- `*toc`: Pointer to the table of contents for the shared memory segment, used to locate specific data structures within the segment
## Dependencies
- Functions called/Symbols referenced:
  - dsm_segment
  - [shm_toc](../s/shm_toc.md)
- Called from (representative examples):
  - [ExecParallelInitializeWorker](../E/ExecParallelInitializeWorker.md)
  - [ParallelQueryMain](ParallelQueryMain.md)
  - [ExecAggInitializeWorker](../E/ExecAggInitializeWorker.md)
  - ExecHashInitializeWorker
  - [ExecSeqScanInitializeWorker](../E/ExecSeqScanInitializeWorker.md)

## Notes and Other Information
- This structure is passed to worker initialization functions for various executor nodes
- Provides a standardized interface for workers to access shared memory regardless of the specific parallel operation type
- Much simpler than ParallelContext since workers don't need to manage other workers or coordinate the overall parallel execution
- Used extensively across different types of parallel executor nodes (scans, joins, aggregations, sorts, etc.)
- The shared memory segment and table of contents are set up by the leader process before workers are launched
- Workers use this context to retrieve their portion of the parallel execution plan and locate shared data structures like tuple queues