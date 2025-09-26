# parallel_vacuum_main

## Location
[src/backend/commands/vacuumparallel.c:987-1104](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/vacuumparallel.c#L987-L1104)

## Overview
Main entry point for parallel vacuum worker processes, responsible for setting up the worker environment and processing indexes assigned to this worker.

## Definition

```c
void
parallel_vacuum_main(dsm_segment *seg, shm_toc *toc)
```
## Detailed Description
This function serves as the main execution routine for parallel vacuum worker processes. It initializes the worker environment by setting up shared memory access, opening relations and indexes, configuring vacuum cost parameters, and establishing error handling. The function then processes the indexes assigned to this worker through parallel vacuum operations.

Key responsibilities include:

**Environment Setup**:
- Validates worker process status (must have PROC_IN_VACUUM flag)
- Sets up shared memory access for coordination with leader and other workers
- Opens the target table and all its indexes with appropriate lock modes
- Configures maintenance_work_mem from shared settings

**Cost-Based Vacuum Delay Configuration**:
- Initializes vacuum cost tracking variables
- Sets up shared cost balance for coordination between workers
- Creates buffer access strategy for this worker

**Worker State Initialization**:
- Populates ParallelVacuumState structure with shared data
- Attaches to shared TidStore for dead tuple information
- Sets up error context for meaningful error reporting

**Index Processing**:
- Processes safe indexes through 
- Tracks buffer and WAL usage during execution
- Reports progress back to shared structures

**Cleanup**:
- Detaches from shared memory structures
- Closes relations and indexes with proper lock modes
- Frees allocated resources

## Parameters / Member Variables
- : DSM segment containing shared memory for parallel vacuum coordination
- : Shared memory table of contents for locating different data structures

## Dependencies
- Functions called/Symbols referenced:
  - [shm_toc_lookup](../s/shm_toc_lookup.md)
  - [table_open](../t/table_open.md)
  - [vac_open_indexes](../v/vac_open_indexes.md)
  - [TidStoreAttach](../T/TidStoreAttach.md)
  - [VacuumUpdateCosts](../V/VacuumUpdateCosts.md)
  - [GetAccessStrategyWithSize](../G/GetAccessStrategyWithSize.md)
  - [parallel_vacuum_error_callback](parallel_vacuum_error_callback.md)
  - [InstrStartParallelQuery](../I/InstrStartParallelQuery.md)
  - [parallel_vacuum_process_safe_indexes](parallel_vacuum_process_safe_indexes.md)
  - [InstrEndParallelQuery](../I/InstrEndParallelQuery.md)
  - [TidStoreDetach](../T/TidStoreDetach.md)
  - [vac_close_indexes](../v/vac_close_indexes.md)
  - [table_close](../t/table_close.md)
  - [FreeAccessStrategy](../F/FreeAccessStrategy.md)
- Called from (representative examples):
  - Background worker process entry point (via parallel worker infrastructure)

## Notes and Other Information
- This is a public function called by the parallel worker infrastructure
- Workers only perform index vacuum/cleanup operations, not heap scanning
- Each worker gets its own buffer access strategy to avoid contention
- Error context is established to provide meaningful error messages specific to the current index being processed
- Progress reporting is disabled for workers since only the leader reports overall progress
- The function assumes indexes are sorted by OID to match the leader's order
- Cost-based vacuum delay is shared among all workers to prevent overwhelming the system
- Buffer and WAL usage tracking allows the leader to aggregate statistics from all workers