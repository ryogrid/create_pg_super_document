# IncrementalSortInfo

## Location
src/include/nodes/execnodes.h: 2361 - 2365

## Overview
IncrementalSortInfo is a structure that holds instrumentation data for incremental sort operations, containing performance metrics for both full sort and prefix sort phases.

## Definition


## Detailed Description
This structure serves as a container for collecting and storing performance instrumentation data during incremental sort operations in PostgreSQL. Incremental sort is an optimization that can sort data in two phases: first by a prefix of the sort keys (prefix sort), then by the remaining keys (full sort). The IncrementalSortInfo structure captures metrics for both phases separately, allowing for detailed analysis of the incremental sort algorithm's performance characteristics.

The structure is primarily used for EXPLAIN ANALYZE output to provide detailed statistics about how the incremental sort operation performed, including memory usage, disk usage, and the number of groups processed in each phase.

## Parameters / Member Variables
- : Performance metrics for the full sort phase, including group count, memory/disk space usage, and sort methods used
- : Performance metrics for the prefix sort phase, including group count, memory/disk space usage, and sort methods used

## Dependencies
- Functions called/Symbols referenced:
  - [IncrementalSortGroupInfo](IncrementalSortGroupInfo.md) (struct type for both member variables)
- Called from (representative examples):
  - [show_incremental_sort_info](../s/show_incremental_sort_info.md) (src/backend/commands/explain.c:3191)
  - [ExecIncrementalSortEstimate](../E/ExecIncrementalSortEstimate.md) (src/backend/executor/nodeIncrementalSort.c:1181)
  - [ExecIncrementalSortInitializeDSM](../E/ExecIncrementalSortInitializeDSM.md) (src/backend/executor/nodeIncrementalSort.c:1203)
  - [ExecIncrementalSortRetrieveInstrumentation](../E/ExecIncrementalSortRetrieveInstrumentation.md) (src/backend/executor/nodeIncrementalSort.c:1242)
  - [SharedIncrementalSortInfo](../S/SharedIncrementalSortInfo.md) (src/include/nodes/execnodes.h:2374)
  - [IncrementalSortState](IncrementalSortState.md) (src/include/nodes/execnodes.h:2403)

## Notes and Other Information
- This structure is part of the PostgreSQL executor node system and is specifically designed for performance monitoring and analysis
- The separation of fullsort and prefixsort metrics allows for fine-grained analysis of incremental sort performance
- Used primarily in EXPLAIN ANALYZE to provide detailed execution statistics to database administrators and developers
- Each IncrementalSortGroupInfo contains detailed metrics including group counts, memory usage, disk usage, and sort methods employed