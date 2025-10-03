# parallel_vacuum_init

## Location
[src/backend/commands/vacuumparallel.c:242-433](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/vacuumparallel.c#L242-L433)

## Overview
Initializes parallel vacuum execution by creating a parallel context, setting up shared memory state, and preparing coordination structures for workers to process vacuum operations on indexes concurrently.

## Definition

```c
ParallelVacuumState *
parallel_vacuum_init(Relation rel, Relation *indrels, int nindexes,
					 int nrequested_workers, int vac_work_mem,
					 int elevel, BufferAccessStrategy bstrategy)
```
## Detailed Description
This function sets up the infrastructure for parallel vacuum operations by:

1. **Worker Computation**: Determines the optimal number of parallel workers based on index characteristics and resource constraints using 
2. **Parallel Context Creation**: Enters parallel mode and creates a parallel context for worker coordination
3. **Shared Memory Setup**: Allocates and initializes shared memory segments for:
   - Index vacuum statistics ()
   - Shared state information () 
   - Dead tuple storage via 
   - Buffer and WAL usage tracking
   - Query text for worker processes
4. **Resource Management**: Configures maintenance work memory distribution among workers and buffer access strategy
5. **Capability Assessment**: Counts indexes supporting different parallel vacuum phases (bulkdel, cleanup, conditional cleanup)

The function returns  if parallel vacuum cannot be performed (e.g., no suitable workers can be allocated).

## Parameters / Member Variables
- `rel`: The heap relation being vacuumed
- `*indrels`: Array of index relations to be processed in parallel
- `nindexes`: Number of indexes in the  array
- `nrequested_workers`: Desired number of parallel workers
- `vac_work_mem`: Memory limit for dead tuple storage (in KB)
- `elevel`: Error level for reporting issues
- `bstrategy`: Buffer access strategy for I/O operations
## Dependencies
- Functions called/Symbols referenced:
  -  - Determines optimal worker count
  -  - Enables parallel execution mode
  -  - Creates parallel worker context
  -  - Creates shared dead tuple storage
  -  /  - Gets handles for shared TidStore
  -  - Initializes dynamic shared memory
  -  functions - Shared memory table-of-contents management
- Called from (representative examples):
  -  (src/backend/access/heap/vacuumlazy.c:2853)

## Notes and Other Information
- Function is located at src/backend/commands/vacuumparallel.c:242-433
- Requires at least one index and non-negative worker request to proceed
- Automatically calculates per-worker maintenance work memory based on indexes using maintenance work memory
- Sets up atomic counters for cost balancing and worker coordination
- Handles both conditional and unconditional parallel cleanup modes for indexes
- Memory allocation uses palloc0 for zero-initialized structures