# ExecParallelHashJoinOuterGetTuple

## Location
[src/backend/executor/nodeHashjoin.c:964-1030](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeHashjoin.c#L964-L1030)

## Overview
ExecParallelHashJoinOuterGetTuple is the parallel variant of outer tuple retrieval for hash joins, handling outer tuple fetching in parallel hash join execution with different behavior for single-batch versus multi-batch scenarios.

## Definition


## Detailed Description
ExecParallelHashJoinOuterGetTuple provides outer tuple retrieval specifically designed for parallel hash join execution. Unlike its non-parallel counterpart, this function handles the complexities of coordinated tuple access across multiple parallel workers.

The function operates under two distinct scenarios:

1. **Single-Batch Execution (nbatch == 1, curbatch == 0)**: 
   - Directly executes the outer plan node to fetch tuples
   - Each parallel worker independently processes tuples from the outer relation
   - Computes hash values and filters NULL-producing tuples
   - Similar to non-parallel behavior but without pre-fetched tuple handling

2. **Multi-Batch Execution**:
   - Uses shared tuple stores (STS) for coordinated access to pre-partitioned outer tuples
   - Calls sts_parallel_scan_next() to get the next tuple from the current batch's shared tuple store
   - Hash values are already computed and stored with the tuples
   - Multiple workers can safely scan the same batch concurrently

The function includes an important coordination mechanism - it sets the outer_eof flag when a batch is exhausted, which helps coordinate batch completion across parallel workers.

## Parameters / Member Variables
- : The outer plan node to fetch tuples from (only used in single-batch case)
- : The HashJoinState containing parallel join execution state
- : Output parameter to store the hash value of the returned tuple

## Dependencies
- Functions called/Symbols referenced:
  - ExecProcNode: Executes outer plan node (single-batch only)
  - TupIsNull: Checks if tuple slot is empty
  - ExecHashGetHashValue: Computes hash value for tuples (single-batch only)
  - sts_parallel_scan_next: Gets next tuple from shared tuple store (multi-batch)
  - [ExecForceStoreMinimalTuple](ExecForceStoreMinimalTuple.md): Converts minimal tuple to slot format
  - ExecClearTuple: Clears tuple slot when no more tuples
  - HJ_FILL_OUTER: Macro to check if this is an outer join

- Called from:
  - [ExecHashJoinImpl](ExecHashJoinImpl.md): Main hash join execution function (parallel path)

## Notes and Other Information
Key differences from non-parallel execution:

- **No Pre-fetched Tuple Handling**: Parallel execution doesn't use the empty-outer optimization that requires pre-fetching, simplifying the logic
- **Shared Tuple Store Integration**: Multi-batch execution relies on the shared tuple store infrastructure for coordinated access to pre-partitioned data
- **Batch EOF Coordination**: Sets outer_eof flag to help coordinate batch completion detection across workers
- **Memory Management**: Uses ExecForceStoreMinimalTuple to safely handle tuples retrieved from shared storage

The function is essential for PostgreSQL's parallel hash join implementation, enabling multiple workers to efficiently coordinate outer tuple processing while maintaining correctness and avoiding race conditions.

**Parallel Coordination**: The use of shared tuple stores ensures that each outer tuple is processed exactly once across all parallel workers, while allowing for efficient concurrent access patterns.

Location: src/backend/executor/nodeHashjoin.c:964-1030