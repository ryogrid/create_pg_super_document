# _brin_leader_participate_as_worker

## Location
src/backend/access/brin/brin.c: 2768 - 2795

## Overview
This function allows the parallel build leader process to participate as a worker in the parallel BRIN index construction, performing the same scanning and building work as other worker processes.

## Definition
```c
static void _brin_leader_participate_as_worker(BrinBuildState *buildstate, Relation heap, Relation index)
```

## Detailed Description
In parallel BRIN index builds, the leader process not only coordinates the parallel operation but also participates as a worker to maximize resource utilization. This function handles the leader's participation by calculating an appropriate memory allocation based on the actual number of participating workers and then calling the common parallel scan and build routine. The memory allocation (sortmem) is calculated by dividing maintenance_work_mem among all participating workers, ensuring fair resource distribution.

## Parameters / Member Variables
- `buildstate`: The BRIN build state containing coordination information and the leader reference
- `heap`: The relation (table) being indexed
- `index`: The BRIN index relation being built

## Dependencies
- Functions called/Symbols referenced:
  - _brin_parallel_scan_and_build: Performs the actual parallel scanning and building work
  - BrinBuildState: Structure containing the build state and leader information
  - BrinLeader: Structure containing parallel coordination data

- Called from (representative examples):
  - _brin_begin_parallel: Calls this function to have the leader participate in the parallel build

## Notes and Other Information
- This is a static function, only accessible within the brin.c file
- The leader uses a potentially higher memory allocation than other workers if fewer workers than requested were actually launched
- The function ensures the leader participates with `true` as the final parameter to _brin_parallel_scan_and_build, indicating leader status
- Memory allocation is based on maintenance_work_mem divided by the actual number of participating workers
- This approach maximizes CPU utilization by having the leader process contribute to the actual index building work rather than just coordinating