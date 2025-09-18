# lazy_cleanup_all_indexes

## Location
src/backend/access/heap/vacuumlazy.c: 2353 - 2420

## Overview
Performs cleanup operations on all indexes of a relation during vacuum, handling both serial and parallel execution modes while tracking progress statistics.

## Definition


## Detailed Description  
This function coordinates the cleanup phase for all indexes on a table during vacuum operations. Index cleanup is the final maintenance step that updates index statistics and metadata after the main vacuuming work is complete. The function handles both serial and parallel execution modes, automatically choosing the appropriate method based on whether parallel vacuum is active.

In serial mode, it iterates through all indexes and calls lazy_cleanup_one_index for each one. In parallel mode, it delegates the work to parallel_vacuum_cleanup_all_indexes to distribute the workload across multiple workers.

The function manages detailed progress reporting throughout the cleanup process, updating statistics on the total number of indexes and the number processed so far. It uses the vacuum relation state to pass tuple count information (both actual and estimated) to the individual index cleanup routines.

## Parameters / Member Variables
- : LVRelState structure containing vacuum operation state, including the array of index relations, current index statistics, tuple counts, and parallel vacuum state

## Dependencies
- Functions called/Symbols referenced:
  - pgstat_progress_update_multi_param
  - ParallelVacuumIsActive
  - lazy_cleanup_one_index  
  - parallel_vacuum_cleanup_all_indexes
  - pgstat_progress_update_param
- Called from:
  - lazy_scan_heap

## Notes and Other Information
- Only executes when do_index_cleanup is enabled and nindexes > 0
- Updates progress reporting to PROGRESS_VACUUM_PHASE_INDEX_CLEANUP phase
- Passes new_rel_tuples count and estimated_count flag to cleanup routines
- The estimated_count flag is set when scanned_pages < rel_pages 
- Resets progress counters to zero after completion
- In serial mode, updates progress counter after each index is processed
- In parallel mode, delegates progress tracking to the parallel cleanup function
- Index cleanup updates index statistics and metadata but doesn't remove tuples
- Critical for maintaining accurate index statistics after vacuum operations