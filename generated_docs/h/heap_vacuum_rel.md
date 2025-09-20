# heap_vacuum_rel

## Location
[src/backend/access/heap/vacuumlazy.c:295-815](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/vacuumlazy.c#L295-L815)

## Overview
heap_vacuum_rel performs VACUUM operation for one heap relation, setting up the environment and orchestrating the entire vacuum process including heap scanning, index maintenance, and statistics updates.

## Definition

```c
void
heap_vacuum_rel(Relation rel, VacuumParams *params,
				BufferAccessStrategy bstrategy)
```
## Detailed Description
heap_vacuum_rel is the main entry point for vacuuming a single heap relation. It performs comprehensive setup and coordination of the vacuum process:

1. **Initialization Phase**: Sets up the LVRelState structure containing all vacuum-related state, initializes error callbacks, opens indexes, and configures vacuum options based on parameters.

2. **Cutoff Determination**: Calculates transaction ID and multixact ID cutoffs that determine which tuples are considered dead and which XIDs/MXIDs should be frozen.

3. **Core Vacuum Work**: Calls lazy_scan_heap to perform the actual heap scanning, pruning, and vacuuming operations.

4. **Post-Processing**: Updates pg_class entries for the relation and its indexes, optionally truncates the relation, and generates comprehensive statistics reports.

The function handles both aggressive and non-aggressive vacuum modes, supports parallel vacuum operations, implements failsafe mechanisms, and provides detailed instrumentation and logging.

## Parameters / Member Variables
- : The heap relation to be vacuumed
- : VacuumParams structure containing vacuum options and settings
- : Buffer access strategy to use during vacuum operations

## Dependencies
- Functions called/Symbols referenced:
  - [lazy_scan_heap](../l/lazy_scan_heap.md) (core vacuum work)
  - [vacuum_get_cutoffs](../v/vacuum_get_cutoffs.md) (cutoff calculations)
  - [dead_items_alloc](../d/dead_items_alloc.md) (memory management)
  - [vac_open_indexes](../v/vac_open_indexes.md) / vac_close_indexes (index management)
  - [lazy_check_wraparound_failsafe](../l/lazy_check_wraparound_failsafe.md) (safety checks)
  - [should_attempt_truncation](../s/should_attempt_truncation.md) / lazy_truncate_heap (relation truncation)
  - [update_relstats_all_indexes](../u/update_relstats_all_indexes.md) (statistics updates)
  - [vac_update_relstats](../v/vac_update_relstats.md) (relation statistics)
  - pgstat_report_vacuum (statistics reporting)

- Called from (representative examples):
  - [SampleHeapTupleVisible](../S/SampleHeapTupleVisible.md) (src/backend/access/heap/heapam_handler.c:2632)
  - HeapScanIsValid (src/include/access/heapam.h:402)

## Notes and Other Information
- The function implements comprehensive error handling with vacuum_error_callback for detailed error reporting
- Supports both verbose and quiet operation modes with detailed logging and instrumentation
- Handles failsafe mechanisms to prevent transaction ID wraparound
- Manages parallel vacuum worker coordination through dead_items_alloc/cleanup
- Updates multiple system catalogs (pg_class) and statistics subsystems
- Implements sophisticated decision-making for index vacuuming bypass optimization
- Source location: src/backend/access/heap/vacuumlazy.c:295-815