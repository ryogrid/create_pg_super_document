# lazy_scan_heap

## Location
src/backend/access/heap/vacuumlazy.c: 816 - 1087

## Overview
lazy_scan_heap is the workhorse function for VACUUM operations, performing the main heap scanning, pruning, index vacuuming coordination, and heap vacuuming in multiple passes.

## Definition


## Detailed Description
lazy_scan_heap orchestrates the core vacuum work through multiple phases:

1. **Initial Heap Pass**: Scans each page in the heap using heap_vac_scan_next_block, performing pruning with lazy_scan_prune or limited processing with lazy_scan_noprune when cleanup locks cannot be acquired. Maintains FSM and visibility map during this pass.

2. **Index Vacuuming**: When dead_items memory fills up or after the initial pass completes, invokes lazy_vacuum to remove index entries pointing to dead heap tuples and perform heap vacuuming.

3. **Memory Management**: Monitors TidStore memory usage and triggers vacuum cycles when approaching memory limits, ensuring progress even with minimal memory.

4. **Progress Reporting**: Updates vacuum progress statistics and performs periodic failsafe checks to prevent transaction wraparound issues.

The function implements a sophisticated two-pass strategy for relations with indexes (initial scan + final vacuum) or an optimized one-pass strategy for heap-only relations. It balances memory usage, I/O efficiency, and maintains critical invariants about index-heap consistency.

## Parameters / Member Variables
- : LVRelState structure containing all vacuum-related state, configuration, and statistics

## Dependencies
- Functions called/Symbols referenced:
  - heap_vac_scan_next_block (block iteration control)
  - lazy_scan_new_or_empty (new/empty page processing)
  - lazy_scan_prune (full page processing with cleanup lock)
  - lazy_scan_noprune (limited page processing without cleanup lock)
  - lazy_vacuum (index and heap vacuuming)
  - lazy_cleanup_all_indexes (final index cleanup)
  - TidStoreMemoryUsage (memory monitoring)
  - lazy_check_wraparound_failsafe (safety checks)
  - FreeSpaceMapVacuumRange (FSM maintenance)
  - visibilitymap_pin (visibility map management)
  - PageGetHeapFreeSpace (free space calculation)
  - vac_estimate_reltuples (tuple statistics)

- Called from (representative examples):
  - heap_vacuum_rel (src/backend/access/heap/vacuumlazy.c:497)

## Notes and Other Information
- Implements memory-conscious processing by monitoring dead_items storage and triggering vacuum cycles when memory limits are approached
- Supports both aggressive and non-aggressive vacuum modes with different processing strategies
- Handles complex locking scenarios, attempting cleanup locks first and falling back to shared locks with limited processing
- Maintains FSM updates strategically - immediate updates for index-less relations or when no second pass is needed
- Performs periodic wraparound failsafe checks every FAILSAFE_EVERY_PAGES blocks
- Updates multiple progress tracking parameters throughout execution
- Source location: src/backend/access/heap/vacuumlazy.c:816-1087