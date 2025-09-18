# ParallelContext

## Location
src/include/access/parallel.h: 31 - 48

## Overview
A comprehensive structure that manages parallel execution context in PostgreSQL, coordinating the lifecycle of parallel worker processes and their shared memory segments.

## Definition


## Detailed Description
ParallelContext is the central management structure for PostgreSQL's parallel execution framework. It encapsulates all necessary information to create, coordinate, and manage parallel worker processes. This includes worker process configuration, shared memory management, error handling, and lifecycle tracking. The structure serves as the main coordination point between the leader process and its parallel workers, maintaining state information about worker processes and providing the infrastructure for inter-process communication through shared memory segments and message queues.

## Parameters / Member Variables
- : Double-linked list node for maintaining ParallelContext instances in a list
- : SubTransaction ID associated with this parallel context
- : Maximum number of worker processes that can be launched
- : Actual number of worker processes to be launched (may be less than nworkers)
- : Number of worker processes that have actually been launched
- : Name of the dynamic library containing the worker function
- : Name of the entry point function for worker processes
- : Stack of error context callbacks for error handling
- : Estimator for calculating shared memory table of contents size
- : Pointer to the dynamic shared memory segment used by parallel workers
- : Private memory area for the parallel context
- : Table of contents for the shared memory segment
- : Array of ParallelWorkerInfo structures, one for each worker
- : Number of workers known to have attached successfully
- : Boolean array tracking which workers have attached

## Dependencies
- Functions called/Symbols referenced:
  - [dlist_node](../d/dlist_node.md)
  - SubTransactionId
  - shm_toc_estimator
  - dsm_segment
  - [shm_toc](../s/shm_toc.md)
  - [ParallelWorkerInfo](ParallelWorkerInfo.md)
- Called from (representative examples):
  - [CreateParallelContext](../C/CreateParallelContext.md)
  - [InitializeParallelDSM](../I/InitializeParallelDSM.md)
  - [LaunchParallelWorkers](../L/LaunchParallelWorkers.md)
  - WaitForParallelWorkersToFinish
  - [ExecInitParallelPlan](../E/ExecInitParallelPlan.md)

## Notes and Other Information
- This structure is the backbone of PostgreSQL's parallel query execution system
- Used extensively in parallel scans, parallel aggregation, parallel hash joins, and other parallel operations
- The structure manages both the setup phase (creating shared memory, launching workers) and the execution phase (coordinating work, handling errors)
- Worker attachment tracking helps ensure all workers are ready before starting parallel execution
- The error context stack enables proper error propagation from worker processes back to the leader
- Integrated with PostgreSQL's transaction system through the SubTransactionId field