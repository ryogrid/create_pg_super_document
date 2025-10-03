# ExecParallelHashJoinSetUpBatches

## Location
[src/backend/executor/nodeHash.c:3104-3183](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeHash.c#L3104-L3183)

## Overview
Sets up shared batch state and tuplestores for parallel hash join operations, initializing batch barriers and creating shared tuplestores for inner and outer relations across all batches.

## Definition

```c
static void
ExecParallelHashJoinSetUpBatches(HashJoinTable hashtable, int nbatch)
```
## Detailed Description
 is responsible for the initial setup of parallel hash join batch infrastructure. This function is typically called by one backend (often the leader) to establish the shared memory structures and synchronization primitives needed for batch processing across multiple parallel workers.

The function performs several critical initialization tasks:
1. **Shared memory allocation**: Allocates space for all batch structures in the DSA area
2. **Batch barrier setup**: Initializes synchronization barriers for coordinating parallel workers across batch phases
3. **Tuplestore initialization**: Creates shared tuplestores for both inner and outer relations for each batch
4. **Accessor creation**: Sets up backend-local accessor structures to interface with shared batch data

For batch 0 (the initial batch), the function performs special handling by immediately advancing through the batch barrier phases since batch 0 doesn't require loading from disk - it's processed directly in memory.

The function ensures that all parallel workers can coordinate effectively during the batch processing phases of parallel hash joins, particularly when memory pressure requires spilling tuples to disk and processing them in multiple batches.

## Parameters / Member Variables
- `hashtable`: HashJoinTable containing parallel state and memory management information
- `nbatch`: Number of batches to set up for the parallel hash join operation
## Dependencies
- Functions called/Symbols referenced:
  - dsa_allocate0 (zero-initialized shared memory allocation)
  - EstimateParallelHashJoinBatch (calculate space needed per batch)
  - [dsa_get_address](../d/dsa_get_address.md) (convert DSA pointer to local address)
  - palloc0_array (allocate zero-initialized array)
  - [BarrierInit](../B/BarrierInit.md), BarrierAttach, BarrierPhase, BarrierArriveAndWait, BarrierDetach (barrier coordination)
  - [sts_initialize](../s/sts_initialize.md) (shared tuplestore initialization)
  - NthParallelHashJoinBatch (get Nth batch from array)
  - ParallelHashJoinBatchInner/Outer (get inner/outer tuplestore areas)
- Called from:
  - [ExecHashTableCreate](ExecHashTableCreate.md) (nodeHash.c:622)
  - [ExecParallelHashIncreaseNumBatches](ExecParallelHashIncreaseNumBatches.md) (nodeHash.c:1148)

## Notes and Other Information
- This is a static function internal to nodeHash.c for parallel hash join batch management
- Only one backend typically calls this function to set up the shared infrastructure
- Other backends use ExecParallelHashEnsureBatchAccessors() to set up their local accessors
- Uses spillCxt memory context for allocating accessor arrays and tuplestore buffers
- Batch 0 receives special treatment as it doesn't need to load data from disk
- Creates separate shared tuplestores for inner and outer relations with unique naming
- The shared tuplestores use SHARED_TUPLESTORE_SINGLE_PASS mode for efficient processing
- [Barrier](../B/Barrier.md) phases coordinate the parallel processing workflow across all participating backends
- The function assumes hashtable->batches is NULL, indicating this is the first batch setup