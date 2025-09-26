# ParallelVacuumState

## Location
[src/backend/commands/vacuumparallel.c:161-241](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/vacuumparallel.c#L161-L241)

## Overview
ParallelVacuumState is the main coordination structure that maintains the complete state for a parallel vacuum operation across multiple worker processes in PostgreSQL.

## Definition

```c
struct ParallelVacuumState
{
	/* NULL for worker processes */
	ParallelContext *pcxt;

	/* Parent Heap Relation */
	Relation	heaprel;

	/* Target indexes */
	Relation   *indrels;
	int			nindexes;

	/* Shared information among parallel vacuum workers */
	PVShared   *shared;

	/*
	 * Shared index statistics among parallel vacuum workers. The array
	 * element is allocated for every index, even those indexes where parallel
	 * index vacuuming is unsafe or not worthwhile (e.g.,
	 * will_parallel_vacuum[] is false).  During parallel vacuum,
	 * IndexBulkDeleteResult of each index is kept in DSM and is copied into
	 * local memory at the end of parallel vacuum.
	 */
	PVIndStats *indstats;

	/* Shared dead items space among parallel vacuum workers */
	TidStore   *dead_items;

	/* Points to buffer usage area in DSM */
	BufferUsage *buffer_usage;

	/* Points to WAL usage area in DSM */
	WalUsage   *wal_usage;

	/*
	 * False if the index is totally unsuitable target for all parallel
	 * processing. For example, the index could be <
	 * min_parallel_index_scan_size cutoff.
	 */
	bool	   *will_parallel_vacuum;

	/*
	 * The number of indexes that support parallel index bulk-deletion and
	 * parallel index cleanup respectively.
	 */
	int			nindexes_parallel_bulkdel;
	int			nindexes_parallel_cleanup;
	int			nindexes_parallel_condcleanup;

	/* Buffer access strategy used by leader process */
	BufferAccessStrategy bstrategy;

	/*
	 * Error reporting state.  The error callback is set only for workers
	 * processes during parallel index vacuum.
	 */
	char	   *relnamespace;
	char	   *relname;
	char	   *indname;
	PVIndVacStatus status;
};
```
## Detailed Description
ParallelVacuumState serves as the central control structure for PostgreSQL's parallel vacuum operations. It coordinates all aspects of parallel vacuum processing, including managing worker processes, tracking index processing states, sharing dead tuple information, and collecting statistics. The structure is used by both the leader process (which orchestrates the operation) and worker processes (which perform the actual vacuum work). It integrates with PostgreSQL's parallel processing framework and shared memory system to enable efficient multi-process vacuum operations.

## Parameters / Member Variables
- `*pcxt`: Parallel context for managing worker processes (NULL for worker processes, only set in leader)
- `heaprel`: Relation object representing the parent heap table being vacuumed
- `*indrels`: Array of Relation objects representing the target indexes to be processed
- `nindexes`: Total number of indexes in the indrels array
- `*shared`: Pointer to PVShared structure containing shared state among all parallel workers
- `*indstats`: Array of PVIndStats structures tracking statistics and status for each index
- `*dead_items`: TidStore containing shared dead tuple identifiers accessible by all workers
- `*buffer_usage`: Pointer to buffer usage statistics area in Dynamic Shared Memory (DSM)
- `*wal_usage`: Pointer to WAL usage statistics area in DSM
- `*will_parallel_vacuum`: Boolean array indicating which indexes are suitable for parallel processing
- `nindexes_parallel_bulkdel`: Count of indexes that support parallel bulk deletion
- `nindexes_parallel_cleanup`: Count of indexes that support parallel cleanup
- `nindexes_parallel_condcleanup`: Count of indexes that support parallel conditional cleanup
- `bstrategy`: Buffer access strategy used by the leader process for efficient I/O
- `*relnamespace`: Namespace name for error reporting (worker processes only)
- `*relname`: Relation name for error reporting (worker processes only)
- `*indname`: Index name for error reporting (worker processes only)
- `status`: Current parallel vacuum status for error callback context

## Dependencies
- Functions called/Symbols referenced:
  - [ParallelContext](ParallelContext.md) (parallel processing framework)
  - [PVShared](PVShared.md) (shared worker state)
  - [PVIndStats](PVIndStats.md) (per-index statistics)
  - [TidStore](../T/TidStore.md) (dead tuple storage)
  - [BufferUsage](../B/BufferUsage.md) (buffer statistics)
  - [WalUsage](../W/WalUsage.md) (WAL statistics)
  - [BufferAccessStrategy](../B/BufferAccessStrategy.md) (I/O strategy)
  - PVIndVacStatus (vacuum status enum)
- Called from (representative examples):
  - [LVRelState](../L/LVRelState.md) (as member in lazy vacuum state)
  - [parallel_vacuum_init](../p/parallel_vacuum_init.md) (initialization)
  - [parallel_vacuum_end](../p/parallel_vacuum_end.md) (cleanup)
  - [parallel_vacuum_process_all_indexes](../p/parallel_vacuum_process_all_indexes.md) (index processing coordination)
  - [parallel_vacuum_main](../p/parallel_vacuum_main.md) (worker main function)
  - [parallel_vacuum_error_callback](../p/parallel_vacuum_error_callback.md) (error handling)

## Notes and Other Information
- The typedef for this structure appears in vacuum.h, making it available to other vacuum-related modules
- Worker processes have pcxt set to NULL since they don't manage the parallel context directly
- The indstats array is allocated for every index, even those not suitable for parallel processing
- [IndexBulkDeleteResult](../I/IndexBulkDeleteResult.md) data is kept in DSM during parallel vacuum and copied to local memory at completion
- Error reporting fields (relnamespace, relname, indname, status) are primarily used by worker processes for context in error callbacks
- Integrates with PostgreSQL's shared memory and parallel processing infrastructure for scalable vacuum operations
- Central to the coordination between leader and worker processes in parallel vacuum workflows