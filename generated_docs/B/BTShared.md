# BTShared

## Location
src/backend/access/nbtree/nbtsort.c: 94 - 151

## Overview
BTShared is a structure that contains status information for B-tree index builds performed in parallel, allocated in a dynamic shared memory segment to coordinate between the leader and worker processes.

## Definition


## Detailed Description
BTShared serves as the central coordination structure for parallel B-tree index construction. It is allocated in dynamic shared memory and shared between the leader process and all worker processes participating in the parallel index build. The structure is divided into immutable fields that are set once during initialization and mutable fields that are updated by workers during the scan phase and aggregated by the leader.

The structure includes a condition variable for synchronizing worker completion and a spinlock mutex to protect access to mutable state. Workers report their progress through the mutable fields, which the leader aggregates to maintain overall build statistics.

## Parameters / Member Variables
- : OID of the heap relation being indexed
- : OID of the index relation being built
- : Whether the index enforces uniqueness constraints
- : Whether NULL values are considered distinct in unique indexes
- : Whether this is a concurrent index build
- : Number of tuplesort states for scanning
- : Condition variable used to monitor worker progress completion
- : Spinlock protecting all mutable fields below
- : Number of worker processes that have finished
- : Total number of input heap tuples processed
- : Whether RECENTLY_DEAD tuples were encountered during build
- : Total number of tuples that made it into the index
- : Whether any worker detected a broken HOT chain during build

## Dependencies
- Functions called/Symbols referenced:
  - ConditionVariable
  - [slock_t](../s/slock_t.md)
- Called from (representative examples):
  - [BTLeader](BTLeader.md)
  - [_bt_begin_parallel](../b/_bt_begin_parallel.md)
  - [_bt_parallel_estimate_shared](../b/_bt_parallel_estimate_shared.md)
  - [_bt_parallel_heapscan](../b/_bt_parallel_heapscan.md)
  - [_bt_parallel_build_main](../b/_bt_parallel_build_main.md)
  - [_bt_parallel_scan_and_sort](../b/_bt_parallel_scan_and_sort.md)

## Notes and Other Information
BTShared is designed specifically for parallel index builds and contains a separate tuplesort TOC entry that is private to tuplesort.c but allocated by the nbtsort module. The structure layout ensures that ParallelTableScanDescData follows immediately after, with consideration for alignment requirements of the parallel table scan descriptor interface. The mutable state fields are only safe to access after all workers have indicated completion through the workersdonecv condition variable.