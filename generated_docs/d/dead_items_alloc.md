# dead_items_alloc

## Location
src/backend/access/heap/vacuumlazy.c: 2823 - 2887

## Overview
Allocates memory for dead item tracking during vacuum operations, supporting both serial and parallel execution modes.

## Definition
```c
static void
dead_items_alloc(LVRelState *vacrel, int nworkers)
```

## Detailed Description
This function handles the allocation of data structures needed to track dead tuples during vacuum operations. It determines the appropriate work memory limit by choosing between autovacuum_work_mem and maintenance_work_mem based on whether the process is an autovacuum worker. For parallel vacuum operations, it initializes parallel vacuum state and allocates dead_items storage in dynamic shared memory (DSM) when multiple workers are available and multiple indexes exist. The function includes validation to prevent parallel vacuum on temporary tables since parallel workers cannot access local buffers. For serial vacuum operations, it allocates the dead_items_info structure locally using palloc and creates a local TidStore for dead item tracking.

## Parameters / Member Variables
- `vacrel`: Vacuum relation state that will be updated with allocated dead items structures
- `nworkers`: Number of parallel workers requested (-1 for serial, 0+ for parallel consideration)

## Dependencies
- Functions called/Symbols referenced:
  - VacDeadItemsInfo
  - AmAutoVacuumWorkerProcess
  - RelationUsesLocalBuffers
  - [parallel_vacuum_init](../p/parallel_vacuum_init.md)
  - ParallelVacuumIsActive
  - [parallel_vacuum_get_dead_items](../p/parallel_vacuum_get_dead_items.md)
  - [TidStoreCreateLocal](../T/TidStoreCreateLocal.md)
  - autovacuum_work_mem
  - maintenance_work_mem
- Called from (representative examples):
  - [heap_vacuum_rel](../h/heap_vacuum_rel.md)

## Notes and Other Information
The function implements intelligent memory management by selecting appropriate work memory limits and choosing between local and shared memory allocation based on parallelism requirements. It ensures that parallel vacuum is only attempted when beneficial (multiple indexes) and safe (non-temporary tables). The parallel vacuum initialization includes proper error handling and resource cleanup. The memory allocation strategy optimizes for the expected vacuum workload while respecting configured memory limits.