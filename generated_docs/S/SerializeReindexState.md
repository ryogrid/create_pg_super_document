# SerializeReindexState

## Location
[src/backend/catalog/index.c:4192-4209](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/index.c#L4192-L4209)

## Overview
Serializes the current reindex state into shared memory for parallel worker processes during parallel reindex operations.

## Definition
```c
void SerializeReindexState(Size maxsize, char *start_address)
```

## Detailed Description
This function takes the current reindex state from global variables and serializes it into a shared memory location that can be accessed by parallel worker processes. It copies the current reindexing context including the heap and index being reindexed, as well as a list of all pending reindexed indexes.

The function populates a SerializedReindexState structure with:
- The currently reindexed heap OID
- The currently reindexed index OID  
- The count of pending reindexed indexes
- An array of all pending reindexed index OIDs

## Parameters / Member Variables
- `maxsize`: The maximum size available in the shared memory buffer (used for bounds checking)
- `start_address`: Pointer to the shared memory location where the serialized state should be written

## Dependencies
- Functions called/Symbols referenced:
  - SerializedReindexState (structure type)
  - list_length (list utility function)
  - lfirst_oid (list cell access macro)
  - currentlyReindexedHeap (global variable)
  - currentlyReindexedIndex (global variable)
  - pendingReindexedIndexes (global list variable)
- Called from (representative examples):
  - [InitializeParallelDSM](../I/InitializeParallelDSM.md)

## Notes and Other Information
- This function is part of PostgreSQL's parallel reindex infrastructure
- The serialized state allows parallel workers to understand the current reindex context
- Global variables like currentlyReindexedHeap and currentlyReindexedIndex track the active reindex operation
- The pendingReindexedIndexes list contains all indexes that are in the process of being reindexed
- Located in src/backend/catalog/index.c at lines 4192-4209
- Works in conjunction with EstimateReindexStateSpace() and RestoreReindexState()