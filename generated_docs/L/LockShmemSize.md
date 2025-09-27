# LockShmemSize

## Location
[src/backend/storage/lmgr/lock.c:3584-3620](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/lock.c#L3584-L3620)

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
  - [LOCK](LOCK.md) (structure size for lock table entries)
  - [PROCLOCK](../P/PROCLOCK.md) (structure size for process-lock association entries)
- Called from (representative examples):
  - [CalculateShmemSize](../C/CalculateShmemSize.md) (during server startup shared memory calculation)

## Notes and Other Information
- The PROCLOCK table is sized at 2x the LOCK table size based on the assumption that multiple processes will typically hold locks on the same objects
- The 10% safety margin is crucial because lock usage can vary significantly based on workload patterns
- This function is called during PostgreSQL startup to determine total shared memory requirements
- The estimation helps prevent shared memory exhaustion during high-concurrency scenarios
- Part of the broader shared memory initialization process coordinated by CalculateShmemSize()

## Simplified Source

```c
// Simplified version of LockShmemSize
Size LockShmemSize(void) {
    Size total_size = 0;
    long estimated_locks;

    // Step 1: Calculate space for main lock hash table
    estimated_locks = NLOCKENTS();
    total_size += hash_estimate_size(estimated_locks, sizeof(LOCK));

    // Step 2: Calculate space for process-lock hash table (2x bigger)
    estimated_locks *= 2;
    total_size += hash_estimate_size(estimated_locks, sizeof(PROCLOCK));

    // Step 3: Add 10% safety margin for estimation uncertainty
    total_size += total_size / 10;

    return total_size;
}
```

Key simplifications made:
- Used more descriptive variable names (total_size, estimated_locks)
- Replaced add_size() calls with += for clarity (assuming no overflow in simplified version)
- Added step-by-step comments explaining the three main phases
- Consolidated the logic flow into clear sequential steps
- Focused on the core algorithm without low-level safety considerations