# ExecScanHashTableForUnmatched

## Location
[src/backend/executor/nodeHash.c:2169-2242](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeHash.c#L2169-L2242)

## Overview
Scans the hash table for unmatched inner tuples during hash join operations, typically used for implementing RIGHT and FULL OUTER joins.

## Definition
bool ExecScanHashTableForUnmatched(HashJoinState *hjstate, ExprContext *econtext)

## Detailed Description
This function systematically scans through all buckets in a hash table to find inner tuples that have not been matched during the main hash join phase. It iterates through regular buckets first, then skew buckets, checking each tuple's match flag. When an unmatched tuple is found, it sets up the execution context and returns the tuple for further processing. The function maintains scanning state across calls using the HashJoinState structure, allowing incremental scanning of large hash tables.

The scanning process follows this order:
1. Regular hash buckets (hjstate->hj_CurBucketNo)
2. Skew buckets for handling hash key distribution outliers
3. Individual tuples within each bucket using the next pointer chain

## Parameters / Member Variables
- : Hash join execution state containing the hash table, current position markers, and tuple slot information
- : Expression evaluation context where the found unmatched tuple will be stored in ecxt_innertuple

## Dependencies
- Functions called/Symbols referenced:
  - HeapTupleHeaderHasMatch - checks if a tuple has been matched
  - HJTUPLE_MINTUPLE - macro to extract minimal tuple from hash join tuple
  - [ExecStoreMinimalTuple](ExecStoreMinimalTuple.md) - stores minimal tuple in a tuple table slot
  - ResetExprContext - resets temporary memory in expression context
  - CHECK_FOR_INTERRUPTS - allows query cancellation
- Called from (representative examples):
  - [ExecHashJoinImpl](ExecHashJoinImpl.md) - [main](../m/main.md) hash join execution function

## Notes and Other Information
- Returns true when an unmatched tuple is found, false when all buckets have been scanned
- The function stores the found tuple in both hjstate->hj_CurTuple and econtext->ecxt_innertuple
- Memory management is handled carefully with ResetExprContext() to prevent memory leaks during long scans
- Supports query cancellation through CHECK_FOR_INTERRUPTS() in the main loop
- Essential for implementing RIGHT and FULL OUTER join semantics in PostgreSQL