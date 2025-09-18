# _brin_begin_parallel

## Location
src/backend/access/brin/brin.c: 2354 - 2537

## Overview
Initializes and launches parallel workers for BRIN index creation, setting up shared memory structures and coordinating parallel heap scanning and sorting operations.

## Definition
```c
static void _brin_begin_parallel(BrinBuildState *buildstate, Relation heap, Relation index, bool isconcurrent, int request)
```

## Detailed Description
This function orchestrates the setup of parallel BRIN index building by:

1. **Creating parallel context**: Establishes a parallel execution environment with the requested number of worker processes
2. **Setting up shared memory**: Allocates and initializes shared data structures including BrinShared state, shared tuplesort state, and usage tracking structures
3. **Configuring parallel table scan**: Initializes the parallel heap scan using appropriate snapshot (SnapshotAny for normal builds, MVCC snapshot for concurrent builds)
4. **Launching worker processes**: Starts the requested number of parallel workers and optionally includes the leader process as a participant
5. **Handling fallback scenarios**: Falls back to serial execution if parallel setup fails

The function creates shared memory segments for coordinating work distribution, collecting statistics, and managing the parallel sorting of BRIN tuples. It handles both regular and concurrent index builds with appropriate snapshot isolation.

## Parameters / Member Variables
- `buildstate`: BRIN build state structure that will be updated with leader information
- `heap`: The heap relation being indexed
- `index`: The BRIN index relation being built
- `isconcurrent`: Boolean flag indicating if this is a CREATE INDEX CONCURRENTLY operation
- `request`: Target number of parallel worker processes to launch

## Dependencies
- Functions called/Symbols referenced:
  - EnterParallelMode (enter parallel execution mode)
  - CreateParallelContext (create parallel worker context)
  - _brin_parallel_estimate_shared (estimate shared memory needs)
  - tuplesort_estimate_shared (estimate tuplesort memory needs)
  - InitializeParallelDSM (initialize dynamic shared memory)
  - table_parallelscan_initialize (setup parallel table scan)
  - tuplesort_initialize_shared (initialize shared tuplesort state)
  - LaunchParallelWorkers (start worker processes)
  - _brin_leader_participate_as_worker (leader participation)
- Called from (representative examples):
  - brinbuild (main BRIN index build function)

## Notes and Other Information
- Falls back to serial execution if no workers can be launched or DSM allocation fails
- Supports both leader participation and leader-only coordination modes
- Handles concurrent builds with MVCC snapshots for consistency
- Allocates shared memory for WAL usage and buffer usage tracking
- Sets up condition variables and spinlocks for worker coordination
- The function is part of PostgreSQL's parallel index building infrastructure
- If DISABLE_LEADER_PARTICIPATION is defined, the leader doesn't participate in scanning