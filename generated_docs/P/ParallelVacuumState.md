# ParallelVacuumState

## Location
src/backend/commands/vacuumparallel.c: 161 - 241

## Overview
ParallelVacuumState is the main coordination structure that maintains the complete state for a parallel vacuum operation across multiple worker processes in PostgreSQL.

## Definition


## Detailed Description
ParallelVacuumState serves as the central control structure for PostgreSQL's parallel vacuum operations. It coordinates all aspects of parallel vacuum processing, including managing worker processes, tracking index processing states, sharing dead tuple information, and collecting statistics. The structure is used by both the leader process (which orchestrates the operation) and worker processes (which perform the actual vacuum work). It integrates with PostgreSQL's parallel processing framework and shared memory system to enable efficient multi-process vacuum operations.

## Parameters / Member Variables
- : Parallel context for managing worker processes (NULL for worker processes, only set in leader)
- : Relation object representing the parent heap table being vacuumed
- : Array of Relation objects representing the target indexes to be processed
- : Total number of indexes in the indrels array
- : Pointer to PVShared structure containing shared state among all parallel workers
- : Array of PVIndStats structures tracking statistics and status for each index
- : TidStore containing shared dead tuple identifiers accessible by all workers
- : Pointer to buffer usage statistics area in Dynamic Shared Memory (DSM)
- : Pointer to WAL usage statistics area in DSM
- : Boolean array indicating which indexes are suitable for parallel processing
- : Count of indexes that support parallel bulk deletion
- : Count of indexes that support parallel cleanup
- : Count of indexes that support parallel conditional cleanup
- : Buffer access strategy used by the leader process for efficient I/O
- : Namespace name for error reporting (worker processes only)
- : Relation name for error reporting (worker processes only)
- : Index name for error reporting (worker processes only)
- : Current parallel vacuum status for error callback context

## Dependencies
- Functions called/Symbols referenced:
  - ParallelContext (parallel processing framework)
  - PVShared (shared worker state)
  - PVIndStats (per-index statistics)
  - TidStore (dead tuple storage)
  - BufferUsage (buffer statistics)
  - WalUsage (WAL statistics)
  - BufferAccessStrategy (I/O strategy)
  - PVIndVacStatus (vacuum status enum)
- Called from (representative examples):
  - LVRelState (as member in lazy vacuum state)
  - parallel_vacuum_init (initialization)
  - parallel_vacuum_end (cleanup)
  - parallel_vacuum_process_all_indexes (index processing coordination)
  - parallel_vacuum_main (worker main function)
  - parallel_vacuum_error_callback (error handling)

## Notes and Other Information
- The typedef for this structure appears in vacuum.h, making it available to other vacuum-related modules
- Worker processes have pcxt set to NULL since they don't manage the parallel context directly
- The indstats array is allocated for every index, even those not suitable for parallel processing
- IndexBulkDeleteResult data is kept in DSM during parallel vacuum and copied to local memory at completion
- Error reporting fields (relnamespace, relname, indname, status) are primarily used by worker processes for context in error callbacks
- Integrates with PostgreSQL's shared memory and parallel processing infrastructure for scalable vacuum operations
- Central to the coordination between leader and worker processes in parallel vacuum workflows