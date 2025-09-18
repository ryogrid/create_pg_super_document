# tuplesort_initialize_shared

## Location
[src/backend/utils/sort/tuplesort.c:2976-2998](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/sort/tuplesort.c#L2976-L2998)

## Overview
Initializes shared tuplesort state that must be established in the leader process before parallel workers are launched.

## Definition
```c
void tuplesort_initialize_shared(Sharedsort *shared, int nWorkers, dsm_segment *seg)
```

## Detailed Description
This function sets up the shared memory structures required for parallel tuple sorting operations. It must be called by the leader process before any worker processes are launched to establish the necessary shared state that all workers will use. The function initializes the shared file set, worker management structures, and tape arrays needed for coordinating parallel sorting operations across multiple worker processes.

## Parameters / Member Variables
- `shared`: Pointer to the Sharedsort structure that will hold the shared state for parallel sorting
- `nWorkers`: Number of worker processes that will participate in the parallel sort (must be > 0)
- `seg`: DSM (Dynamic Shared Memory) segment used for shared file operations

## Dependencies
- Functions called/Symbols referenced:
  - SpinLockInit
  - SharedFileSetInit
  - dsm_segment
  - Sharedsort
- Called from (representative examples):
  - [_brin_begin_parallel](../b/_brin_begin_parallel.md)
  - [_bt_begin_parallel](../b/_bt_begin_parallel.md)

## Notes and Other Information
- Must be called before tuplesort_attach_shared() is used by worker processes
- The nWorkers parameter should match the argument passed to tuplesort_estimate_shared()
- Initializes the shared mutex, worker counters, and tape structures for parallel coordination
- Sets up one tape per worker process for result management