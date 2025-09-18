# restore_toc_entries_parallel

## Location
src/bin/pg_dump/pg_backup_archiver.c: 4277 - 4394

## Overview
Main engine for the second phase of parallel restore, dispatching TOC entries to parallel worker processes/threads while respecting dependencies and restore passes.

## Definition


## Detailed Description
This function implements the core parallel processing phase of PostgreSQL's restore operation. It manages a pool of worker processes (on Unix) or threads (on Windows), each with separate database connections, to restore database objects in parallel while maintaining dependency relationships. The function uses a binary heap to efficiently manage items ready for processing and coordinates with workers through a sophisticated job dispatch and completion system.

The function operates in multiple restore passes (RESTORE_PASS_MAIN through RESTORE_PASS_LAST), allowing items with different processing requirements to be handled appropriately. It continuously cycles through dispatching ready work items, waiting for workers to complete jobs, and updating dependencies until all restorable items are processed.

## Parameters / Member Variables
- `AH`: ArchiveHandle containing restore context, TOC entries, and configuration
- `pstate`: ParallelState managing the pool of worker processes/threads and their status
- `pending_list`: TocEntry list containing all items that need restoration but may be blocked by dependencies

## Dependencies
- Functions called/Symbols referenced:
  - pg_log_debug, pg_log_info (logging functions for debug and informational messages)
  - [binaryheap_allocate](../b/binaryheap_allocate.md), binaryheap_free, binaryheap_empty (binary heap management for ready items)
  - [TocEntrySizeCompareBinaryheap](../T/TocEntrySizeCompareBinaryheap.md) (comparator function for heap ordering)
  - [move_to_ready_heap](../m/move_to_ready_heap.md) (moves dependency-satisfied items from pending list to ready heap)
  - [pop_next_work_item](../p/pop_next_work_item.md) (retrieves next available work item from ready heap)
  - [DispatchJobForTocEntry](../D/DispatchJobForTocEntry.md) (dispatches restoration job to available worker)
  - [mark_restore_job_done](../m/mark_restore_job_done.md) (callback function executed when worker completes job)
  - [reduce_dependencies](reduce_dependencies.md) (updates dependency counts after item completion)
  - [IsEveryWorkerIdle](../I/IsEveryWorkerIdle.md) (checks if all workers are currently idle)
  - [WaitForWorkers](../W/WaitForWorkers.md) (waits for worker completion with various strategies)
  - REQ_SCHEMA, REQ_DATA (requirement flags for determining if item needs restoration)
  - ACT_RESTORE (action constant for restoration jobs)
  - RESTORE_PASS_MAIN, RESTORE_PASS_LAST (restore pass constants)
  - WFW_ONE_IDLE, WFW_GOT_STATUS (wait-for-workers strategy constants)
- Called from (representative examples):
  - [RestoreArchive](../R/RestoreArchive.md) (main restore orchestration function)

## Notes and Other Information
- Second phase of three-phase parallel restore system
- Uses binary heap for efficient priority-based work item selection
- Supports both process-based (Unix) and thread-based (Windows) parallelization
- Maintains database connection per worker for true parallel execution
- Implements multi-pass restore strategy to handle different types of dependencies
- Skips items that don't require restoration (neither schema nor data) but still updates their dependencies
- Uses sophisticated wait strategies to balance responsiveness with resource usage
- Guarantees at least one idle worker is available before dispatching new jobs to prevent blocking
- Processes items in dependency-respecting order while maximizing parallel opportunities
- Handles edge cases like items with no remaining dependencies and transition between restore passes