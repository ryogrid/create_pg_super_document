# EstimateReindexStateSpace

## Location
[src/backend/catalog/index.c:4181-4191](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/index.c#L4181-L4191)

## Overview
Estimates the memory space needed to serialize reindex state information for passing to parallel worker processes during parallel reindex operations.

## Definition
```c
Size EstimateReindexStateSpace(void)
```

## Detailed Description
This function calculates the amount of memory required to store the serialized reindex state that needs to be shared with parallel workers during a reindex operation. The calculation is based on the size of the SerializedReindexState structure plus space for an array of pending reindexed index OIDs.

The function computes the total size by adding:
- The base size of SerializedReindexState up to the flexible array member
- The size needed for the array of pending reindexed index OIDs

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - SerializedReindexState (structure type)
  - [mul_size](../m/mul_size.md) (utility function for safe size multiplication)
  - [list_length](../l/list_length.md) (list utility function)
  - pendingReindexedIndexes (global list variable)
- Called from (representative examples):
  - [InitializeParallelDSM](../I/InitializeParallelDSM.md)

## Notes and Other Information
- This function is part of PostgreSQL's parallel reindex infrastructure
- The estimation is used to allocate shared memory for parallel worker communication
- Uses mul_size() for overflow-safe multiplication when calculating array sizes
- The pendingReindexedIndexes is a global list that tracks indexes currently being reindexed
- Located in src/backend/catalog/index.c at lines 4181-4191

## Simplified Source

```c
Size EstimateReindexStateSpace(void) {
    // Base size of SerializedReindexState up to flexible array member
    // plus space for array of pending reindexed index OIDs
    return offsetof(SerializedReindexState, pendingReindexedIndexes)
           + mul_size(sizeof(Oid), list_length(pendingReindexedIndexes));
}
```