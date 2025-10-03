# ExecMergeTupleDumpInner

## Location
[src/backend/executor/nodeMergejoin.c:558-569](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeMergejoin.c#L558-L569)

## Overview
ExecMergeTupleDumpInner is a debug utility function that prints the current inner tuple in a merge join operation to stdout for debugging purposes.

## Definition

```c
static void
ExecMergeTupleDumpInner(MergeJoinState *mergestate)
```
## Detailed Description
ExecMergeTupleDumpInner is a debugging utility function within the merge join executor that provides formatted output of the current inner tuple. This function serves as a companion to ExecMergeTupleDumpOuter, specifically focusing on the inner relation's tuple during merge join processing.

The function examines the inner tuple slot in the merge join state and prints either the tuple contents using MJ_debugtup or "(nil)" if the slot is empty. This debugging capability is essential for developers working on merge join algorithms, allowing them to trace the progression of tuples from the inner relation and verify that the join logic is processing data correctly.

## Parameters / Member Variables
- `*mergestate`: MergeJoinState structure containing the merge join execution state, specifically accessing the inner tuple slot
## Dependencies
- Functions called/Symbols referenced:
  - [MergeJoinState](../M/MergeJoinState.md) (merge join state structure)
  - TupIsNull (macro to check if tuple slot is null)
  - MJ_debugtup (debug macro to print tuple contents)
  - printf (standard C library function for formatted output)
- Called from (representative examples):
  - [ExecMergeTupleDump](ExecMergeTupleDump.md) (general tuple dumping function that calls this for inner tuple display)

## Notes and Other Information
- This is a debug-only function typically compiled conditionally based on debug build settings
- Part of a comprehensive debugging suite for merge join operations including functions for outer and marked tuples
- The output format includes a header "==== inner tuple ====" to distinguish it from outer tuple output in debug logs  
- Used primarily during PostgreSQL development and debugging rather than in production environments
- The function assumes the mergestate parameter is valid and contains a properly initialized inner tuple slot
- Provides crucial visibility into inner relation tuple processing during complex merge join operations