# CreateParallelContext

## Location
[src/backend/access/transam/parallel.c:169-206](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/parallel.c#L169-L206)

## Overview
Creates a new parallel execution context for coordinating parallel workers, initializing the necessary data structures to manage a group of background worker processes.

## Definition

```c
enumslen = 0;
```
## Detailed Description
CreateParallelContext establishes a new parallel context that serves as the foundation for parallel query execution in PostgreSQL. This function must be called after entering parallel mode and creates a ParallelContext structure that will manage the lifecycle of parallel workers. The function allocates memory in the TopTransactionContext to ensure the context persists for the duration of the transaction, initializes tracking structures for shared memory estimation, and registers the context in the global context list for cleanup purposes.

The parallel context tracks the library and function name that workers will execute, manages the number of workers to launch, and maintains error context information. It also initializes a shared memory table-of-contents estimator that will be used to calculate space requirements for worker communication.

## Parameters / Member Variables
- : Name of the dynamic library containing the worker entry point function
- : Name of the function that parallel workers will execute as their main entry point
- : Number of parallel worker processes to create (must be non-negative)

## Dependencies
- Functions called/Symbols referenced:
  - IsInParallelMode (validates parallel mode is active)
  - [GetCurrentSubTransactionId](../G/GetCurrentSubTransactionId.md) (tracks transaction context)
  - shm_toc_initialize_estimator (initializes shared memory estimator)
  - [dlist_push_head](../d/dlist_push_head.md) (adds context to global list)
  - [ParallelContext](../P/ParallelContext.md) (structure type being created)

- Called from (representative examples):
  - [_brin_begin_parallel](../b/_brin_begin_parallel.md) (BRIN index operations)
  - [_bt_begin_parallel](../b/_bt_begin_parallel.md) (B-tree index operations)
  - [parallel_vacuum_init](../p/parallel_vacuum_init.md) (parallel vacuum operations)
  - [ExecInitParallelPlan](../E/ExecInitParallelPlan.md) (parallel query execution)

## Notes and Other Information
- Must be called only after entering parallel mode (enforced by assertion)
- The context must be destroyed before exiting the current subtransaction
- Memory is allocated in TopTransactionContext to ensure proper lifetime management
- The function initializes but does not launch the actual worker processes
- Error context stack is preserved to maintain proper error reporting in workers
- The context is automatically tracked in a global list for cleanup purposes