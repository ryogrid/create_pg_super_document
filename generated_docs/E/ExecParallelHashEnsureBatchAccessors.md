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
- `hashtable`: HashJoinTable containing parallel state and local accessor information that needs to be synchronized
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

## Simplified Source

```c
static void
ExecParallelHashEnsureBatchAccessors(HashJoinTable hashtable)
{
    ParallelHashJoinState *pstate = hashtable->parallel_state;
    ParallelHashJoinBatch *batches;

    // Early return if accessor count matches shared state
    if (hashtable->batches != NULL)
    {
        if (hashtable->nbatch == pstate->nbatch)
            return;
        ExecParallelHashCloseBatchAccessors(hashtable);
    }

    // Validate shared batch array exists
    Assert(DsaPointerIsValid(pstate->batches));

    // Switch to spill memory context for accessor allocation
    MemoryContext oldcxt = MemoryContextSwitchTo(hashtable->spillCxt);

    // Allocate local accessor array matching shared batch count
    hashtable->nbatch = pstate->nbatch;
    hashtable->batches = palloc0_array(ParallelHashJoinBatchAccessor, hashtable->nbatch);

    // Get base address of shared batch array
    batches = (ParallelHashJoinBatch *) dsa_get_address(hashtable->area, pstate->batches);

    // Initialize accessor for each batch and attach to tuplestores
    for (int i = 0; i < hashtable->nbatch; ++i)
    {
        ParallelHashJoinBatchAccessor *accessor = &hashtable->batches[i];
        ParallelHashJoinBatch *shared = NthParallelHashJoinBatch(batches, i);

        accessor->shared = shared;
        accessor->preallocated = 0;
        accessor->done = false;
        accessor->outer_eof = false;

        // Attach to existing shared tuplestores
        accessor->inner_tuples = sts_attach(ParallelHashJoinBatchInner(shared),
                                          ParallelWorkerNumber + 1,
                                          &pstate->fileset);
        accessor->outer_tuples = sts_attach(ParallelHashJoinBatchOuter(shared, pstate->nparticipants),
                                          ParallelWorkerNumber + 1,
                                          &pstate->fileset);
    }

    MemoryContextSwitchTo(oldcxt);
}
```

This simplified version shows the core accessor synchronization: check if update needed, allocate new accessor array matching current shared batch count, and attach to existing shared tuplestores for each batch.