# ExecParallelHashEnsureBatchAccessors

## Location
[src/backend/executor/nodeHash.c:3205-3268](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeHash.c#L3205-L3268)

## Overview
Ensures that the current backend has up-to-date batch accessor structures that match the current shared batch state, handling dynamic changes in batch count during parallel hash join execution.

## Definition

```c
static void
ExecParallelHashEnsureBatchAccessors(HashJoinTable hashtable)
```
## Detailed Description
 is a synchronization function that ensures each parallel worker backend maintains consistent access to the current set of batches in a parallel hash join. This function is critical when the number of batches changes dynamically due to memory pressure or other factors during hash join execution.

The function performs several key operations:
1. **Consistency check**: Compares the local backend's batch count with the shared state to determine if updates are needed
2. **Cleanup existing accessors**: If the batch count has changed, it properly closes existing batch accessors using 
3. **Accessor recreation**: Allocates and initializes new accessor arrays that match the current shared batch configuration
4. **Tuplestore attachment**: Attaches to existing shared tuplestores for both inner and outer relations across all batches

This function is essential for maintaining consistency across parallel workers when batch structures are modified, such as during batch count increases triggered by memory constraints. Each backend calls this function to ensure its local accessors remain synchronized with the globally shared batch state.

## Parameters / Member Variables
- : HashJoinTable containing parallel state and local accessor information that needs to be synchronized

## Dependencies
- Functions called/Symbols referenced:
  - [ExecParallelHashCloseBatchAccessors](ExecParallelHashCloseBatchAccessors.md) (cleanup existing accessors)
  - DsaPointerIsValid (validate shared batch array pointer)
  - palloc0_array (allocate zero-initialized accessor array)  
  - [dsa_get_address](../d/dsa_get_address.md) (convert DSA pointer to local address)
  - NthParallelHashJoinBatch (access specific batch in array)
  - [sts_attach](../s/sts_attach.md) (attach to existing shared tuplestores)
  - ParallelHashJoinBatchInner/Outer (get tuplestore memory areas)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md) (manage memory contexts)
- Called from:
  - [MultiExecParallelHash](../M/MultiExecParallelHash.md) (nodeHash.c:278, 341)
  - [ExecParallelHashIncreaseNumBatches](ExecParallelHashIncreaseNumBatches.md) (nodeHash.c:1226, 1252)
  - [ExecParallelHashIncreaseNumBuckets](ExecParallelHashIncreaseNumBuckets.md) (nodeHash.c:1585)

## Notes and Other Information
- This is a static function internal to nodeHash.c for parallel hash join coordination
- Early return optimization when batch count hasn't changed (hashtable->nbatch == pstate->nbatch)
- Uses spillCxt memory context for allocating accessor structures and tuplestore buffers
- Initializes accessor state including preallocated=0, done=false, outer_eof=false
- Attaches to existing shared tuplestores rather than creating new ones (unlike ExecParallelHashJoinSetUpBatches)
- Critical for handling dynamic batch count changes during parallel hash join execution
- Each backend must call this function to maintain synchronized access to shared batch structures  
- The function assumes shared batch array is valid (asserts DsaPointerIsValid)
- Pairs with ExecParallelHashCloseBatchAccessors() for proper resource management