# lazy_vacuum_all_indexes

## Location
src/backend/access/heap/vacuumlazy.c: 1990 - 2106

## Overview
Main entry point for index vacuuming that coordinates the removal of dead tuple references from all indexes, supporting both serial and parallel execution modes.

## Definition
```c
static bool lazy_vacuum_all_indexes(LVRelState *vacrel)
```

## Detailed Description
lazy_vacuum_all_indexes orchestrates the index vacuuming phase of the VACUUM operation, responsible for removing references to dead tuples from all indexes on the relation. The function implements comprehensive wraparound failsafe checking before, during, and after index processing to prevent transaction ID wraparound failures. It supports both serial execution (processing indexes one by one) and parallel execution (delegating to parallel vacuum workers). The function maintains detailed progress reporting for monitoring and includes sophisticated error handling for emergency scenarios where the vacuum operation must be terminated early to prevent system-wide transaction ID wraparound.

## Parameters / Member Variables
- `vacrel`: LVRelState containing the complete VACUUM operation state, including index information, dead items, and execution configuration

## Dependencies
- Functions called/Symbols referenced:
  - lazy_check_wraparound_failsafe
  - pgstat_progress_update_multi_param
  - ParallelVacuumIsActive
  - lazy_vacuum_one_index
  - pgstat_progress_update_param
  - parallel_vacuum_bulkdel_all_indexes
  - PROGRESS_VACUUM_PHASE
  - PROGRESS_VACUUM_INDEXES_TOTAL
  - PROGRESS_VACUUM_INDEXES_PROCESSED
  - PROGRESS_VACUUM_NUM_INDEX_VACUUMS
  - PROGRESS_VACUUM_PHASE_VACUUM_INDEX
- Called from:
  - lazy_vacuum

## Notes and Other Information
- Returns true if all indexes were successfully vacuumed, false if wraparound failsafe triggered
- Performs multiple wraparound failsafe checks: pre-check, during processing, and post-check
- Updates progress reporting with detailed index processing metrics
- For parallel vacuum, delegates bulk delete operations to parallel_vacuum_bulkdel_all_indexes
- Increments num_index_scans counter even for incomplete rounds due to failsafe activation
- Serial mode processes indexes sequentially with per-index wraparound checking
- Maintains IndexBulkDeleteResult statistics for each index vacuum operation
- Critical for preventing transaction ID wraparound by monitoring vacuum duration and progress