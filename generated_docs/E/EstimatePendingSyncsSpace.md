# EstimatePendingSyncsSpace

## Location
[src/backend/catalog/storage.c:571-583](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/storage.c#L571-L583)

## Overview
EstimatePendingSyncsSpace estimates the amount of shared memory space needed to pass pending sync information to parallel worker processes.

## Definition
```c
Size EstimatePendingSyncsSpace(void)
```

## Detailed Description
EstimatePendingSyncsSpace calculates the memory space required to serialize and pass pending sync information from the leader process to parallel workers. This is part of PostgreSQL's parallel query execution infrastructure, specifically for handling relations that are skipping WAL logging.

The function works by:
1. Determining the number of entries in the pendingSyncHash (0 if no hash exists)
2. Calculating space needed for each RelFileLocator entry plus one additional entry
3. Using mul_size() to safely compute the total size while avoiding integer overflow

This size estimation is used during parallel query setup to allocate sufficient shared memory for communicating which relations are currently skipping WAL logging. Parallel workers need this information to properly handle buffer management and ensure data consistency during operations on relations that use the "Skipping WAL for New RelFileLocator" optimization.

## Parameters / Member Variables
None - this function takes no parameters

## Dependencies
- Functions called/Symbols referenced:
  - [hash_get_num_entries](../h/hash_get_num_entries.md)
  - [mul_size](../m/mul_size.md)
  - pendingSyncHash (global variable)
  - [RelFileLocator](../R/RelFileLocator.md) (type)
- Called from (representative examples):
  - [InitializeParallelDSM](../I/InitializeParallelDSM.md)

## Notes and Other Information
- Part of the parallel query execution infrastructure
- Returns 0 when no pending syncs exist (no pendingSyncHash)
- Uses mul_size() for safe integer arithmetic to prevent overflow
- The "+1" in the calculation accounts for sentinel/terminator entries
- Critical for ensuring parallel workers have complete information about WAL-skipping relations
- Used during dynamic shared memory (DSM) setup for parallel operations
- Ensures data consistency across parallel worker processes handling bulk operations