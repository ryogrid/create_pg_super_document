# restore_toc_entries_parallel

## Location
[src/bin/pg_dump/pg_backup_archiver.c:4277-4394](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_backup_archiver.c#L4277-L4394)

## Overview
Main engine for the second phase of parallel restore, dispatching TOC entries to parallel worker processes/threads while respecting dependencies and restore passes.

## Definition

```c
static void
restore_toc_entries_parallel(ArchiveHandle *AH, ParallelState *pstate,
							 TocEntry *pending_list)
```
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

## Simplified Source

```c
static void
restore_toc_entries_parallel(ArchiveHandle *AH, ParallelState *pstate,
                             TocEntry *pending_list)
{
    binaryheap *ready_heap;
    TocEntry   *next_work_item;

    pg_log_debug("entering restore_toc_entries_parallel");

    // Set up binary heap for ready items, ordered by size for load balancing
    ready_heap = binaryheap_allocate(AH->tocCount,
                                     TocEntrySizeCompareBinaryheap,
                                     NULL);

    // Move items with no dependencies to ready_heap
    AH->restorePass = RESTORE_PASS_MAIN;
    move_to_ready_heap(pending_list, ready_heap, AH->restorePass);

    // Main parallel processing loop
    pg_log_info("entering main parallel loop");

    for (;;)
    {
        // Get next ready item from heap
        next_work_item = pop_next_work_item(ready_heap, pstate);

        if (next_work_item != NULL)
        {
            // Skip items that don't need restoration
            if ((next_work_item->reqs & (REQ_SCHEMA | REQ_DATA)) == 0)
            {
                pg_log_info("skipping item %d %s %s",
                           next_work_item->dumpId,
                           next_work_item->desc, next_work_item->tag);

                // Update dependencies as if completed
                reduce_dependencies(AH, next_work_item, ready_heap);
                continue;
            }

            pg_log_info("launching item %d %s %s",
                       next_work_item->dumpId,
                       next_work_item->desc, next_work_item->tag);

            // Dispatch job to available worker
            DispatchJobForTocEntry(AH, pstate, next_work_item, ACT_RESTORE,
                                   mark_restore_job_done, ready_heap);
        }
        else if (IsEveryWorkerIdle(pstate))
        {
            // No ready items and all workers idle
            if (AH->restorePass == RESTORE_PASS_LAST)
                break;  // Completely done

            // Advance to next restore pass
            AH->restorePass++;
            move_to_ready_heap(pending_list, ready_heap, AH->restorePass);
            continue;
        }
        // else: nothing ready but workers are busy, wait for completion

        // Wait for workers to finish jobs and update dependencies
        // Strategy: if we dispatched work, wait for one idle worker
        //          if no work dispatched, wait for any status update
        WaitForWorkers(AH, pstate,
                       next_work_item ? WFW_ONE_IDLE : WFW_GOT_STATUS);
    }

    // Cleanup
    Assert(binaryheap_empty(ready_heap));
    binaryheap_free(ready_heap);

    pg_log_info("finished main parallel loop");
}
```