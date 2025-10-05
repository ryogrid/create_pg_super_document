# lazy_scan_heap

## Location
[src/backend/access/heap/vacuumlazy.c:816-1087](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/vacuumlazy.c#L816-L1087)

## Overview
lazy_scan_heap is the workhorse function for VACUUM operations, performing the main heap scanning, pruning, index vacuuming coordination, and heap vacuuming in multiple passes.

## Definition

```c
static void
lazy_scan_heap(LVRelState *vacrel)
```
## Detailed Description
lazy_scan_heap orchestrates the core vacuum work through multiple phases:

1. **Initial Heap Pass**: Scans each page in the heap using heap_vac_scan_next_block, performing pruning with lazy_scan_prune or limited processing with lazy_scan_noprune when cleanup locks cannot be acquired. Maintains FSM and visibility map during this pass.

2. **Index Vacuuming**: When dead_items memory fills up or after the initial pass completes, invokes lazy_vacuum to remove index entries pointing to dead heap tuples and perform heap vacuuming.

3. **Memory Management**: Monitors TidStore memory usage and triggers vacuum cycles when approaching memory limits, ensuring progress even with minimal memory.

4. **Progress Reporting**: Updates vacuum progress statistics and performs periodic failsafe checks to prevent transaction wraparound issues.

The function implements a sophisticated two-pass strategy for relations with indexes (initial scan + final vacuum) or an optimized one-pass strategy for heap-only relations. It balances memory usage, I/O efficiency, and maintains critical invariants about index-heap consistency.

## Parameters / Member Variables
- `*vacrel`: LVRelState structure containing all vacuum-related state, configuration, and statistics
## Dependencies
- Functions called/Symbols referenced:
  - [heap_vac_scan_next_block](../h/heap_vac_scan_next_block.md) (block iteration control)
  - [lazy_scan_new_or_empty](lazy_scan_new_or_empty.md) (new/empty page processing)
  - [lazy_scan_prune](lazy_scan_prune.md) (full page processing with cleanup lock)
  - [lazy_scan_noprune](lazy_scan_noprune.md) (limited page processing without cleanup lock)
  - [lazy_vacuum](lazy_vacuum.md) (index and heap vacuuming)
  - [lazy_cleanup_all_indexes](lazy_cleanup_all_indexes.md) (final index cleanup)
  - [TidStoreMemoryUsage](../T/TidStoreMemoryUsage.md) (memory monitoring)
  - [lazy_check_wraparound_failsafe](lazy_check_wraparound_failsafe.md) (safety checks)
  - [FreeSpaceMapVacuumRange](../F/FreeSpaceMapVacuumRange.md) (FSM maintenance)
  - [visibilitymap_pin](../v/visibilitymap_pin.md) (visibility map management)
  - [PageGetHeapFreeSpace](../P/PageGetHeapFreeSpace.md) (free space calculation)
  - [vac_estimate_reltuples](../v/vac_estimate_reltuples.md) (tuple statistics)

- Called from (representative examples):
  - [heap_vacuum_rel](../h/heap_vacuum_rel.md) (src/backend/access/heap/vacuumlazy.c:497)

## Notes and Other Information
- Implements memory-conscious processing by monitoring dead_items storage and triggering vacuum cycles when memory limits are approached
- Supports both aggressive and non-aggressive vacuum modes with different processing strategies
- Handles complex locking scenarios, attempting cleanup locks first and falling back to shared locks with limited processing
- Maintains FSM updates strategically - immediate updates for index-less relations or when no second pass is needed
- Performs periodic wraparound failsafe checks every FAILSAFE_EVERY_PAGES blocks
- Updates multiple progress tracking parameters throughout execution
- Source location: src/backend/access/heap/vacuumlazy.c:816-1087

## Simplified Source

```c
static void lazy_scan_heap(LVRelState *vacrel) {
    BlockNumber rel_pages = vacrel->rel_pages;
    BlockNumber blkno, next_fsm_block_to_vacuum = 0;
    bool all_visible_according_to_vm;
    Buffer vmbuffer = InvalidBuffer;

    // Report progress - starting heap scan phase
    pgstat_progress_update_multi_param(3, initprog_index, initprog_val);

    // Initialize block scanning state
    vacrel->current_block = InvalidBlockNumber;
    vacrel->next_unskippable_block = InvalidBlockNumber;

    // Main scanning loop - process each block in the heap
    while (heap_vac_scan_next_block(vacrel, &blkno, &all_visible_according_to_vm)) {
        Buffer buf;
        Page page;
        int ndeleted = 0;
        bool has_lpdead_items;
        bool got_cleanup_lock = false;

        vacrel->scanned_pages++;

        // Update progress and error tracking info
        pgstat_progress_update_param(PROGRESS_VACUUM_HEAP_BLKS_SCANNED, blkno);
        update_vacuum_error_info(vacrel, NULL, VACUUM_ERRCB_PHASE_SCAN_HEAP,
                                blkno, InvalidOffsetNumber);

        vacuum_delay_point();

        // Periodic wraparound failsafe check
        if (vacrel->scanned_pages % FAILSAFE_EVERY_PAGES == 0)
            lazy_check_wraparound_failsafe(vacrel);

        // Check if we need to vacuum indexes/heap due to memory pressure
        if (vacrel->dead_items_info->num_items > 0 &&
            TidStoreMemoryUsage(vacrel->dead_items) > vacrel->dead_items_info->max_bytes) {

            // Release visibility map buffer before long operation
            if (BufferIsValid(vmbuffer)) {
                ReleaseBuffer(vmbuffer);
                vmbuffer = InvalidBuffer;
            }

            // Perform index and heap vacuuming cycle
            vacrel->consider_bypass_optimization = false;
            lazy_vacuum(vacrel);

            // Vacuum FSM for the range we've processed so far
            FreeSpaceMapVacuumRange(vacrel->rel, next_fsm_block_to_vacuum, blkno);
            next_fsm_block_to_vacuum = blkno;

            // Resume heap scanning
            pgstat_progress_update_param(PROGRESS_VACUUM_PHASE, PROGRESS_VACUUM_PHASE_SCAN_HEAP);
        }

        // Pin visibility map page for potential all-visible marking
        visibilitymap_pin(vacrel->rel, blkno, &vmbuffer);

        // Read the heap page
        buf = ReadBufferExtended(vacrel->rel, MAIN_FORKNUM, blkno, RBM_NORMAL, vacrel->bstrategy);
        page = BufferGetPage(buf);

        // Try to get cleanup lock, fall back to shared lock if needed
        got_cleanup_lock = ConditionalLockBufferForCleanup(buf);
        if (!got_cleanup_lock)
            LockBuffer(buf, BUFFER_LOCK_SHARE);

        // Handle new or empty pages
        if (lazy_scan_new_or_empty(vacrel, buf, blkno, page, !got_cleanup_lock, vmbuffer)) {
            continue;  // Page processed, move to next
        }

        // Process page based on lock type acquired
        if (!got_cleanup_lock &&
            !lazy_scan_noprune(vacrel, buf, blkno, page, &has_lpdead_items)) {
            // Need full processing - upgrade to cleanup lock
            LockBuffer(buf, BUFFER_LOCK_UNLOCK);
            LockBufferForCleanup(buf);
            got_cleanup_lock = true;
        }

        // Full page processing with cleanup lock
        if (got_cleanup_lock)
            ndeleted = lazy_scan_prune(vacrel, buf, blkno, page,
                                     vmbuffer, all_visible_according_to_vm,
                                     &has_lpdead_items);

        // Update FSM immediately for certain cases
        if (vacrel->nindexes == 0 || !vacrel->do_index_vacuuming || !has_lpdead_items) {
            Size freespace = PageGetHeapFreeSpace(page);
            UnlockReleaseBuffer(buf);
            RecordPageWithFreeSpace(vacrel->rel, blkno, freespace);

            // Periodic FSM vacuuming for index-less relations
            if (got_cleanup_lock && vacrel->nindexes == 0 && ndeleted > 0 &&
                blkno - next_fsm_block_to_vacuum >= VACUUM_FSM_EVERY_PAGES) {
                FreeSpaceMapVacuumRange(vacrel->rel, next_fsm_block_to_vacuum, blkno);
                next_fsm_block_to_vacuum = blkno;
            }
        } else {
            UnlockReleaseBuffer(buf);
        }
    }

    // Cleanup scanning state
    vacrel->blkno = InvalidBlockNumber;
    if (BufferIsValid(vmbuffer))
        ReleaseBuffer(vmbuffer);

    // Report scan completion
    pgstat_progress_update_param(PROGRESS_VACUUM_HEAP_BLKS_SCANNED, blkno);

    // Compute final tuple statistics
    vacrel->new_live_tuples = vac_estimate_reltuples(vacrel->rel, rel_pages,
                                                    vacrel->scanned_pages,
                                                    vacrel->live_tuples);

    vacrel->new_rel_tuples = Max(vacrel->new_live_tuples, 0) +
                           vacrel->recently_dead_tuples +
                           vacrel->missed_dead_tuples;

    // Final index and heap vacuuming if needed
    if (vacrel->dead_items_info->num_items > 0)
        lazy_vacuum(vacrel);

    // Final FSM cleanup
    if (blkno > next_fsm_block_to_vacuum)
        FreeSpaceMapVacuumRange(vacrel->rel, next_fsm_block_to_vacuum, blkno);

    pgstat_progress_update_param(PROGRESS_VACUUM_HEAP_BLKS_VACUUMED, blkno);

    // Final index cleanup phase
    if (vacrel->nindexes > 0 && vacrel->do_index_cleanup)
        lazy_cleanup_all_indexes(vacrel);
}
```