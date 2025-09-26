# ExecParallelHashJoinPartitionOuter

## Location
src/backend/executor/nodeHashjoin.c: 1498 - 1543

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
  - ExecProcNode
  - TupIsNull
  - ExecHashGetHashValue
  - HJ_FILL_OUTER
  - ExecFetchSlotMinimalTuple
  - ExecHashGetBucketAndBatch
  - sts_puttuple
  - heap_free_minimal_tuple
  - sts_end_write
  - MinimalTuple (type)
  - HashJoinTable (type)
- Called from (representative examples):
  - ExecHashJoinImpl

## Notes and Other Information
- This is a static function used internally within the hash join implementation
- Assumes hj_FirstOuterTupleSlot is NULL, indicating a fresh partitioning operation
- Uses MinimalTuple format for memory-efficient tuple storage in shared tuplestores
- Includes interrupt checking (CHECK_FOR_INTERRUPTS) to allow for query cancellation during long-running operations
- The sts_end_write calls ensure that all parallel workers can read from the outer partition tuplestores
- Essential for parallel hash join performance as it enables work distribution across multiple processes