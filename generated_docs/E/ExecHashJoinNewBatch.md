# ExecHashJoinNewBatch

## Location
src/backend/executor/nodeHashjoin.c: 1031 - 1171

## Overview
Switches to a new hashjoin batch when the current batch has been completed, managing the transition between batches in a multi-batch hash join operation.

## Definition


## Detailed Description
This function manages the complex process of transitioning from one batch to the next in a hash join operation. Hash joins may be divided into multiple batches when the inner relation is too large to fit entirely in memory. The function handles cleanup of the previous batch, determines which batches can be skipped based on outer join requirements and optimization rules, reloads the hash table with the new inner batch data, and prepares the outer batch file for reading.

The function implements several optimization strategies:
- Skips completely empty batches on both sides when possible
- Handles special cases for outer joins where empty batches still need processing
- Manages skew optimization state transitions after the first batch
- Accounts for dynamic batch number increases during execution

## Parameters / Member Variables
- : The HashJoinState containing all state information for the hash join operation, including the hash table and batch file references

## Dependencies
- Functions called/Symbols referenced:
  - BufFileClose
  - HJ_FILL_OUTER
  - HJ_FILL_INNER
  - [ExecHashTableReset](ExecHashTableReset.md)
  - BufFileSeek
  - [ExecHashJoinGetSavedTuple](ExecHashJoinGetSavedTuple.md)
  - ExecHashTableInsert
- Called from (representative examples):
  - [ExecHashJoinImpl](ExecHashJoinImpl.md)

## Notes and Other Information
- Returns true if a new batch was successfully prepared, false if no more batches remain
- Implements three key rules for determining when batches can be skipped: outer join requirements, dynamic batch increases during inner scan, and dynamic batch increases during outer scan
- Manages memory cleanup by closing previous batch files and resetting skew optimization state
- The function is static and only used internally within the hash join executor node
- Critical for memory management in large hash join operations that exceed available memory