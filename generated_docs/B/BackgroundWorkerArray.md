# BackgroundWorkerArray

## Location
src/backend/postmaster/bgworker.c: 94 - 100

## Overview
BackgroundWorkerArray is a shared memory structure that manages an array of background worker slots and maintains counters for parallel worker registration and termination to enforce the max_parallel_workers limit.

## Definition


## Detailed Description
BackgroundWorkerArray serves as the central registry for all background worker processes in PostgreSQL's shared memory. It implements a dual-counter system to track parallel worker limits without requiring locks that could compromise postmaster stability.

The parallel worker counting mechanism uses two separate counters:
- parallel_register_count: Incremented by backends when registering parallel workers (protected by BackgroundWorkerLock)
- parallel_terminate_count: Incremented by postmaster when parallel workers terminate (lockless)

The active parallel worker count is calculated as (registered - terminated), allowing enforcement of max_parallel_workers GUC setting. The counters are designed to handle overflow safely since only the difference matters for the calculation.

## Parameters / Member Variables
- : Total number of available background worker slots in the array
- : Counter of registered parallel workers, modified by backends under BackgroundWorkerLock protection
- : Counter of terminated parallel workers, modified only by postmaster without locks
- : Flexible array of BackgroundWorkerSlot structures containing the actual worker slot data

## Dependencies
- Functions called/Symbols referenced:
  - BackgroundWorkerSlot
  - FLEXIBLE_ARRAY_MEMBER
- Called from (representative examples):
  - BackgroundWorkerHandle
  - BackgroundWorkerShmemSize

## Notes and Other Information
- Uses flexible array member for efficient memory layout - actual size determined at runtime
- The dual-counter design prevents postmaster from taking locks while still maintaining accurate parallel worker limits  
- Counter overflow is explicitly handled and considered safe since only the difference is meaningful
- Part of PostgreSQL's shared memory infrastructure for background worker management
- Enforces max_parallel_workers GUC through the parallel worker counting mechanism
- Critical for preventing resource exhaustion in heavily parallel workloads