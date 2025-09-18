# ExecPrepHashTableForUnmatched

## Location
src/backend/executor/nodeHash.c: 2083 - 2103

## Overview
Initializes the hash join state to prepare for scanning the hash table to find unmatched inner tuples, typically used in RIGHT and FULL outer join operations.

## Definition
```c
void ExecPrepHashTableForUnmatched(HashJoinState *hjstate)
```

## Detailed Description
This function sets up the necessary state in HashJoinState to begin a systematic scan of the entire hash table looking for inner tuples that have not been matched during the main hash join phase. This is a crucial step in implementing RIGHT JOIN and FULL OUTER JOIN operations, where unmatched inner tuples need to be returned with NULL values for the outer relation columns.

The function initializes the scan state by:
1. Setting the current bucket number to 0 to start from the first regular bucket
2. Setting the current skew bucket number to 0 to start from the first skew bucket  
3. Setting the current tuple to NULL to indicate the start of a fresh scan

This initialization enables subsequent calls to ExecScanHashTableForUnmatched to systematically traverse all buckets and skew buckets in the hash table to locate unmatched tuples.

## Parameters / Member Variables
- `hjstate`: Hash join state structure that will be configured for unmatched tuple scanning

## Dependencies
- Functions called/Symbols referenced:
  - HashJoinState (struct type)
- Called from (representative examples):
  - ExecParallelPrepHashTableForUnmatched (calls this function)
  - ExecHashJoinImpl

## Notes and Other Information
- This function is used specifically for RIGHT JOIN and FULL OUTER JOIN operations
- The scan state fields are repurposed from their normal hash join usage for this specialized scanning mode
- Must be called before any calls to ExecScanHashTableForUnmatched
- The function is very lightweight, only performing state initialization with no complex logic
- Works in conjunction with the hash table's match flags that track which tuples have been joined during the main phase