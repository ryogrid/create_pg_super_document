# ParallelContext

## Location
[src/include/access/parallel.h:31-48](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/parallel.h#L31-L48)

## Overview
A comprehensive structure that manages parallel execution context in PostgreSQL, coordinating the lifecycle of parallel worker processes and their shared memory segments.

## Definition

```c
typedef struct ParallelContext
{
	dlist_node	node;
	SubTransactionId subid;
	int			nworkers;		/* Maximum number of workers to launch */
	int			nworkers_to_launch; /* Actual number of workers to launch */
	int			nworkers_launched;
	char	   *library_name;
	char	   *function_name;
	ErrorContextCallback *error_context_stack;
	shm_toc_estimator estimator;
	dsm_segment *seg;
	void	   *private_memory;
	shm_toc    *toc;
	ParallelWorkerInfo *worker;
	int			nknown_attached_workers;
	bool	   *known_attached_workers;
} ParallelContext;
```
## Detailed Description
ParallelContext is the central management structure for PostgreSQL's parallel execution framework. It encapsulates all necessary information to create, coordinate, and manage parallel worker processes. This includes worker process configuration, shared memory management, error handling, and lifecycle tracking. The structure serves as the main coordination point between the leader process and its parallel workers, maintaining state information about worker processes and providing the infrastructure for inter-process communication through shared memory segments and message queues.

## Parameters / Member Variables
- `node`: Double-linked list node for maintaining ParallelContext instances in a list
- `subid`: SubTransaction ID associated with this parallel context
- `nworkers`: Maximum number of worker processes that can be launched
- `nworkers_to_launch`: Actual number of worker processes to be launched (may be less than nworkers)
- `nworkers_launched`: Number of worker processes that have actually been launched
- `*library_name`: Name of the dynamic library containing the worker function
- `*function_name`: Name of the entry point function for worker processes
- `*error_context_stack`: Stack of error context callbacks for error handling
- `estimator`: Estimator for calculating shared memory table of contents size
- `*seg`: Pointer to the dynamic shared memory segment used by parallel workers
- `*private_memory`: Private memory area for the parallel context
- `*toc`: Table of contents for the shared memory segment
- `*worker`: Array of ParallelWorkerInfo structures, one for each worker
- `nknown_attached_workers`: Number of workers known to have attached successfully
- `*known_attached_workers`: Boolean array tracking which workers have attached
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