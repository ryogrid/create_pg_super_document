# ExecHashJoinImpl

## Location
src/backend/executor/nodeHashjoin.c: 220 - 677

## Overview
ExecHashJoinImpl implements the core Hybrid Hash Join algorithm for PostgreSQL, handling both parallel and non-parallel execution through an inline-optimized state machine that processes hash joins for all join types.

## Definition


## Detailed Description
ExecHashJoinImpl is the heart of PostgreSQL's hash join execution engine, implementing the Hybrid Hash Join algorithm through a comprehensive state machine. The function is marked with  to allow compilers to create specialized versions for parallel and non-parallel execution, optimizing away unnecessary branches.

The algorithm operates by building a hash table on the "inner" relation and probing it with tuples from the "outer" relation. It handles complex scenarios including:

- Multi-batch processing when memory is insufficient for the entire hash table
- Empty outer relation optimization to avoid unnecessary hash table construction
- Various join types (inner, left, right, anti, semi, full)
- Parallel execution coordination through barriers
- Skew bucket handling for performance optimization
- Unmatched tuple processing for outer joins

The state machine progresses through several key states:
1. **HJ_BUILD_HASHTABLE**: Initial hash table construction
2. **HJ_NEED_NEW_OUTER**: Fetching outer tuples for probing
3. **HJ_SCAN_BUCKET**: Scanning hash buckets for matches
4. **HJ_FILL_OUTER_TUPLE**: Handling unmatched outer tuples in outer joins
5. **HJ_FILL_INNER_TUPLES**: Processing unmatched inner tuples
6. **HJ_NEED_NEW_BATCH**: Advancing to next batch in multi-batch scenarios

## Parameters / Member Variables
- : The PlanState node containing hash join execution state
- : Boolean flag indicating whether to use parallel hash join execution paths

## Dependencies
- Functions called/Symbols referenced:
  - ExecHashTableCreate: Creates the hash table structure
  - ExecHashJoinOuterGetTuple/ExecParallelHashJoinOuterGetTuple: Retrieves outer tuples
  - ExecScanHashBucket/ExecParallelScanHashBucket: Scans hash buckets for matches
  - ExecHashJoinNewBatch/ExecParallelHashJoinNewBatch: Advances to next batch
  - ExecQual: Evaluates join and filter conditions
  - ExecProject: Projects result tuples
  - BarrierArriveAndWait: Synchronizes parallel workers

- Called from:
  - ExecHashJoin: Non-parallel hash join entry point
  - ExecParallelHashJoin: Parallel hash join entry point

## Notes and Other Information
The function implements several important optimizations:
- **Empty outer optimization**: Checks if outer relation is empty before building hash table for certain join types
- **Batch processing**: Handles cases where hash table doesn't fit in memory by processing data in batches
- **Parallel coordination**: Uses barriers to coordinate parallel workers during hash table construction and batch processing
- **Skew handling**: Special processing for highly skewed data using dedicated skew buckets

The always-inline attribute is crucial for performance, allowing the compiler to eliminate the parallel branch checks in specialized contexts, reducing runtime overhead.

Location: src/backend/executor/nodeHashjoin.c:220-677