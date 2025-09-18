# BrinShared

## Location
src/backend/access/brin/brin.c: 57 - 105

## Overview
BrinShared is a structure that stores status information for BRIN index builds performed in parallel, allocated in a dynamic shared memory segment to coordinate between leader and worker processes.

## Definition


## Detailed Description
BrinShared serves as the central coordination structure for parallel BRIN index builds. It contains both immutable configuration data that worker processes need to replicate the leader's state, and mutable status fields that track the progress of the parallel build operation. The structure is designed to be allocated in dynamic shared memory, allowing multiple processes to coordinate their work on building a BRIN index.

The structure includes a condition variable for monitoring worker progress and a mutex for protecting shared state updates. Workers report their completion status and tuple counts back to the leader through this shared structure.

## Parameters / Member Variables
- : OID of the heap relation being indexed
- : OID of the BRIN index being built
- : Flag indicating whether this is a concurrent index build
- : Number of pages per BRIN range for this index
- : Number of scan tuple sort states
- : Condition variable used to monitor worker process completion
- : Spinlock protecting mutable fields in the structure
- : Number of worker processes that have finished their work
- : Total number of input heap tuples processed
- : Total number of tuples that were successfully inserted into the index

## Dependencies
- Functions called/Symbols referenced:
  - ConditionVariable
  - slock_t
- Called from (representative examples):
  - ParallelTableScanFromBrinShared
  - BrinLeader
  - _brin_begin_parallel
  - _brin_parallel_heapscan
  - _brin_parallel_estimate_shared

## Notes and Other Information
The structure is followed by ParallelTableScanDescData which cannot be directly embedded due to potential alignment requirements. The mutex protects all mutable fields, ensuring thread-safe updates to progress tracking information. The condition variable allows the leader to efficiently wait for all workers to complete before proceeding with index finalization.