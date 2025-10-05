# ExecParallelHashJoinPartitionOuter

## Location
[src/backend/executor/nodeHashjoin.c:1498-1543](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeHashjoin.c#L1498-L1543)

## Overview
Partitions outer relation tuples across multiple shared tuplestores for parallel hash join processing by reading all outer tuples and distributing them based on hash values.

## Definition
static void ExecParallelHashJoinPartitionOuter(HashJoinState *hjstate)

## Detailed Description
ExecParallelHashJoinPartitionOuter is responsible for partitioning the outer relation tuples in a parallel hash join operation. It reads all tuples from the outer plan and distributes them across multiple shared tuplestores based on their hash values, enabling parallel workers to process different partitions independently.

The function performs the following key operations:
1. Executes the outer plan to retrieve all tuples
2. For each tuple, computes a hash value using the join keys
3. Determines the appropriate batch and bucket based on the hash value
4. Converts tuples to minimal tuple format for efficient storage
5. Stores tuples in the corresponding batch's outer tuplestore
6. Finalizes all outer partition tuplestores for reading by parallel workers

This partitioning strategy allows parallel hash join workers to process their assigned partitions without contention, improving overall performance.

## Parameters / Member Variables
- `hjstate`: Pointer to the HashJoinState structure containing the hash join execution state

## Dependencies
- Functions called/Symbols referenced:
  - outerPlanState
  - [ExecProcNode](ExecProcNode.md)
  - TupIsNull
  - [ExecHashGetHashValue](ExecHashGetHashValue.md)
  - HJ_FILL_OUTER
  - [ExecFetchSlotMinimalTuple](ExecFetchSlotMinimalTuple.md)
  - [ExecHashGetBucketAndBatch](ExecHashGetBucketAndBatch.md)
  - [sts_puttuple](../s/sts_puttuple.md)
  - [heap_free_minimal_tuple](../h/heap_free_minimal_tuple.md)
  - [sts_end_write](../s/sts_end_write.md)
  - MinimalTuple (type)
  - [HashJoinTable](../H/HashJoinTable.md) (type)
- Called from (representative examples):
  - [ExecHashJoinImpl](ExecHashJoinImpl.md)

## Notes and Other Information
- This is a static function used internally within the hash join implementation
- Assumes hj_FirstOuterTupleSlot is NULL, indicating a fresh partitioning operation
- Uses MinimalTuple format for memory-efficient tuple storage in shared tuplestores
- Includes interrupt checking (CHECK_FOR_INTERRUPTS) to allow for query cancellation during long-running operations
- The sts_end_write calls ensure that all parallel workers can read from the outer partition tuplestores
- Essential for parallel hash join performance as it enables work distribution across multiple processes

## Simplified Source

```c
static void
ExecParallelHashJoinPartitionOuter(HashJoinState *hjstate)
{
    PlanState *outerState = outerPlanState(hjstate);
    ExprContext *econtext = hjstate->js.ps.ps_ExprContext;
    HashJoinTable hashtable = hjstate->hj_HashTable;
    TupleTableSlot *slot;
    uint32 hashvalue;
    int i;

    Assert(hjstate->hj_FirstOuterTupleSlot == NULL);

    // Read all outer tuples and partition them by hash value
    for (;;) {
        slot = ExecProcNode(outerState);
        if (TupIsNull(slot))
            break;

        // Set up expression context and compute hash value
        econtext->ecxt_outertuple = slot;
        if (ExecHashGetHashValue(hashtable, econtext,
                               hjstate->hj_OuterHashKeys,
                               true, HJ_FILL_OUTER(hjstate),
                               &hashvalue)) {
            int batchno, bucketno;
            bool shouldFree;
            MinimalTuple mintup = ExecFetchSlotMinimalTuple(slot, &shouldFree);

            // Determine target batch and store tuple
            ExecHashGetBucketAndBatch(hashtable, hashvalue, &bucketno, &batchno);
            sts_puttuple(hashtable->batches[batchno].outer_tuples,
                        &hashvalue, mintup);

            // Clean up minimal tuple if it was allocated
            if (shouldFree)
                heap_free_minimal_tuple(mintup);
        }

        CHECK_FOR_INTERRUPTS();
    }

    // Finalize all outer partition tuplestores for parallel reading
    for (i = 0; i < hashtable->nbatch; ++i)
        sts_end_write(hashtable->batches[i].outer_tuples);
}
```