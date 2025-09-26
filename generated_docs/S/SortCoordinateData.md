# SortCoordinateData

## Location
[src/include/utils/tuplesort.h:45-59](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/tuplesort.h#L45-L59)

## Overview
SortCoordinateData is a structure used for coordinating parallel tuple sorting operations among multiple worker processes and a leader process in PostgreSQL.

## Definition
```c
typedef struct SortCoordinateData
{
    /* Worker process?  If not, must be leader. */
    bool        isWorker;

    /*
     * Leader-process-passed number of participants known launched (workers
     * set this to -1).  Includes state within leader needed for it to
     * participate as a worker, if any.
     */
    int         nParticipants;

    /* Private opaque state (points to shared memory) */
    Sharedsort *sharedsort;
} SortCoordinateData;
```

## Detailed Description
SortCoordinateData serves as the local coordination state for parallel tuplesort operations. Each participant process (both leader and workers) maintains its own instance of this structure in local memory. The structure facilitates communication and synchronization between processes during parallel sorting by maintaining role information and providing access to shared memory state through the Sharedsort pointer.

The parallel tuplesort architecture uses a leader-worker model where the leader process coordinates multiple worker processes to sort data in parallel, then merges the results. This structure contains the essential information each process needs to participate in this coordinated effort.

## Parameters / Member Variables
- `isWorker`: Boolean flag indicating whether this process is a worker (true) or the leader process (false)
- `nParticipants`: Total number of participating processes including the leader; workers set this to -1 as they receive this information from the leader
- `sharedsort`: Pointer to shared memory containing the mutable state of the parallel sort operation, including synchronization primitives and worker coordination data

## Dependencies
- Functions called/Symbols referenced:
  - [Sharedsort](Sharedsort.md)
- Called from (representative examples):
  - [brinbuild](../b/brinbuild.md) (src/backend/access/brin/brin.c:1178)
  - [_brin_parallel_scan_and_build](../b/_brin_parallel_scan_and_build.md) (src/backend/access/brin/brin.c:2807)
  - [_bt_spools_heapscan](../b/_bt_spools_heapscan.md) (src/backend/access/nbtree/nbtsort.c:399,457)
  - [_bt_parallel_scan_and_sort](../b/_bt_parallel_scan_and_sort.md) (src/backend/access/nbtree/nbtsort.c:1873,1900)

## Notes and Other Information
- This structure is allocated by each participant in local memory, not shared memory
- The participant caller is responsible for initializing all fields
- Used in parallel index builds (BRIN, B-tree) to coordinate sorting operations across multiple processes
- The typedef SortCoordinate is a pointer to this structure for easier handling in function signatures