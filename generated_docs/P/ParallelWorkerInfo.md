# ParallelWorkerInfo

## Location
[src/include/access/parallel.h:25-29](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/parallel.h#L25-L29)

## Overview
A structure that contains information about a parallel worker process, including its background worker handle and error message queue handle for communication.

## Definition

```c
typedef struct ParallelWorkerInfo
{
	BackgroundWorkerHandle *bgwhandle;
	shm_mq_handle *error_mqh;
} ParallelWorkerInfo;
```
## Detailed Description
ParallelWorkerInfo is a simple data structure used in PostgreSQL's parallel processing framework to track information about individual worker processes. It serves as a container for essential handles needed to manage and communicate with background worker processes that are part of a parallel operation. This structure is typically used within the broader parallel execution context to maintain references to worker processes and their communication channels.

## Parameters / Member Variables
- `*bgwhandle`: A pointer to the BackgroundWorkerHandle that represents the actual background worker process
- `*error_mqh`: A pointer to the shared memory message queue handle used for receiving error messages from the worker process
## Dependencies
- Functions called/Symbols referenced:
  - [BackgroundWorkerHandle](../B/BackgroundWorkerHandle.md)
  - [shm_mq_handle](../s/shm_mq_handle.md)
- Called from (representative examples):
  - [InitializeParallelDSM](../I/InitializeParallelDSM.md)
  - [ParallelContext](ParallelContext.md)

## Notes and Other Information
- This structure is part of PostgreSQL's parallel query execution infrastructure
- The error message queue handle allows the main process to receive and handle errors that occur in worker processes
- Used as a component within the larger ParallelContext structure for managing multiple parallel workers