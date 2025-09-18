# RestoreReindexState

## Location
src/backend/catalog/index.c: 4210 - 4229

## Overview
Restores reindex state from serialized data in parallel worker processes to synchronize their reindex context with the leader process.

## Definition
```c
void RestoreReindexState(const void *reindexstate)
```

## Detailed Description
This function is called in parallel worker processes to restore the reindex state that was serialized by the leader process. It deserializes the reindex context from shared memory and reconstructs the global state variables that track the current reindex operation.

The function performs the following operations:
- Restores the currently reindexed heap and index OIDs to global variables
- Rebuilds the pendingReindexedIndexes list from the serialized array
- Sets the reindexing transaction nesting level to the current worker's transaction level
- Uses TopMemoryContext to ensure the restored list persists for the worker's lifetime

## Parameters / Member Variables
- `reindexstate`: Pointer to the serialized reindex state data in shared memory (cast to SerializedReindexState internally)

## Dependencies
- Functions called/Symbols referenced:
  - SerializedReindexState (structure type)
  - lappend_oid (list utility function to append OID values)
  - GetCurrentTransactionNestLevel (transaction utility function)
  - MemoryContextSwitchTo (memory management function)
  - TopMemoryContext (global memory context)
  - currentlyReindexedHeap (global variable)
  - currentlyReindexedIndex (global variable)
  - pendingReindexedIndexes (global list variable)
  - reindexingNestLevel (global variable)
- Called from (representative examples):
  - ParallelWorkerMain

## Notes and Other Information
- This function is part of PostgreSQL's parallel reindex infrastructure
- Called only in parallel worker processes, not in the leader process
- Includes an assertion that pendingReindexedIndexes is initially NIL in workers
- Uses TopMemoryContext to ensure the restored index list survives for the worker's lifetime
- The reindexingNestLevel is set to the worker's own transaction nesting level, not the leader's
- Located in src/backend/catalog/index.c at lines 4210-4229
- Works in conjunction with EstimateReindexStateSpace() and SerializeReindexState()
- Critical for maintaining consistency between leader and worker processes during parallel reindex operations