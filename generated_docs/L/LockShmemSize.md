# LockShmemSize

## Location
src/backend/storage/lmgr/lock.c: 3584 - 3620

## Overview
LockShmemSize estimates the amount of shared memory space required for the lock management hash tables.

## Definition
```c
Size LockShmemSize(void)
```

## Detailed Description
This function calculates the estimated shared memory requirements for PostgreSQL's lock management system. It computes the space needed for two main hash tables that store lock-related information:

1. **Lock Hash Table**: Uses NLOCKENTS() to determine the maximum number of lock objects and calculates space for LOCK structures
2. **PROCLOCK Hash Table**: Assumes twice as many PROCLOCK entries as LOCK entries (since multiple processes can hold locks on the same object) and calculates space for PROCLOCK structures

The function applies a 10% safety margin to account for the fact that NLOCKENTS() provides only an estimate, ensuring sufficient shared memory allocation even under heavier-than-expected lock usage scenarios.

The calculation uses PostgreSQL's standard shared memory sizing functions (hash_estimate_size, add_size) to ensure proper alignment and overflow protection.

## Parameters / Member Variables
This function takes no parameters and returns a Size value representing the estimated shared memory requirement.

## Dependencies
- Functions called/Symbols referenced:
  - NLOCKENTS (macro/function that estimates lock table size)
  - [hash_estimate_size](../h/hash_estimate_size.md) (calculates hash table memory requirements)
  - [add_size](../a/add_size.md) (safely adds Size values with overflow checking)
  - LOCK (structure size for lock table entries)
  - [PROCLOCK](../P/PROCLOCK.md) (structure size for process-lock association entries)
- Called from (representative examples):
  - [CalculateShmemSize](../C/CalculateShmemSize.md) (during server startup shared memory calculation)

## Notes and Other Information
- The PROCLOCK table is sized at 2x the LOCK table size based on the assumption that multiple processes will typically hold locks on the same objects
- The 10% safety margin is crucial because lock usage can vary significantly based on workload patterns
- This function is called during PostgreSQL startup to determine total shared memory requirements
- The estimation helps prevent shared memory exhaustion during high-concurrency scenarios
- Part of the broader shared memory initialization process coordinated by CalculateShmemSize()