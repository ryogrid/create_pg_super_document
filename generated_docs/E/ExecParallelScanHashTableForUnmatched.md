# ExecParallelScanHashTableForUnmatched

## Location
[src/backend/executor/nodeHash.c:2243-2305](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeHash.c#L2243-L2305)

## Overview
Scans the hash table for unmatched inner tuples during parallel hash join operations, providing thread-safe access for concurrent workers in parallel hash joins.

## Definition
bool ExecParallelScanHashTableForUnmatched(HashJoinState *hjstate, ExprContext *econtext)

## Detailed Description
This function is the parallel-aware version of ExecScanHashTableForUnmatched, designed to work safely in multi-worker parallel hash join scenarios. It systematically scans through hash table buckets to find inner tuples that haven't been matched, but uses specialized parallel hash functions to ensure thread-safe access to shared hash table data. Unlike the single-worker version, it doesn't handle skew buckets separately since parallel hash tables manage data distribution differently.

The function coordinates with other parallel workers through:
1. Thread-safe bucket traversal using ExecParallelHashFirstTuple/ExecParallelHashNextTuple
2. Proper synchronization of bucket scanning across workers
3. Safe access to shared hash table structures

## Parameters / Member Variables
- : Hash join execution state containing the parallel hash table and current scanning position
- : Expression evaluation context where found unmatched tuples are stored for processing

## Dependencies
- Functions called/Symbols referenced:
  - [ExecParallelHashNextTuple](ExecParallelHashNextTuple.md) - thread-safe function to get next tuple in parallel hash table
  - [ExecParallelHashFirstTuple](ExecParallelHashFirstTuple.md) - thread-safe function to get first tuple in a bucket
  - HeapTupleHeaderHasMatch - checks if a tuple has been matched during join
  - HJTUPLE_MINTUPLE - macro to extract minimal tuple from hash join tuple
  - [ExecStoreMinimalTuple](ExecStoreMinimalTuple.md) - stores minimal tuple in tuple table slot
  - ResetExprContext - resets temporary memory in expression context
  - CHECK_FOR_INTERRUPTS - allows query cancellation
- Called from (representative examples):
  - [ExecHashJoinImpl](ExecHashJoinImpl.md) - [main](../m/main.md) hash join execution function for parallel workers

## Notes and Other Information
- Designed specifically for parallel hash join execution with multiple worker processes
- Uses specialized parallel hash table access functions to ensure thread safety
- Does not handle skew buckets like the single-worker version, as parallel hash tables use different optimization strategies
- Returns true when an unmatched tuple is found, false when scanning is complete
- Essential for implementing RIGHT and FULL OUTER joins in parallel query execution
- Maintains the same memory management patterns as the single-worker version