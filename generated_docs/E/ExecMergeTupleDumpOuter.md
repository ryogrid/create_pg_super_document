# ExecMergeTupleDumpOuter

## Location
[src/backend/executor/nodeMergejoin.c:546-557](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeMergejoin.c#L546-L557)

## Overview
ExecMergeTupleDumpOuter is a debug utility function that prints the current outer tuple in a merge join operation to stdout for debugging purposes.

## Definition

```c
static void
ExecMergeTupleDumpOuter(MergeJoinState *mergestate)
```
## Detailed Description
ExecMergeTupleDumpOuter is a debugging utility function within the merge join executor that provides formatted output of the current outer tuple. This function is typically used during development and debugging to inspect the state of merge join operations by displaying the outer tuple content.

The function checks if the outer tuple slot contains a valid tuple and prints either the tuple contents (using MJ_debugtup) or "(nil)" if the slot is empty. This helps developers understand the data flow and tuple processing during merge join execution, particularly when troubleshooting join logic or performance issues.

## Parameters / Member Variables
- `*mergestate`: MergeJoinState structure containing the merge join execution state, specifically accessing the outer tuple slot
## Dependencies
- Functions called/Symbols referenced:
  - [MergeJoinState](../M/MergeJoinState.md) (merge join state structure)
  - TupIsNull (macro to check if tuple slot is null)
  - MJ_debugtup (debug macro to print tuple contents)
  - printf (standard C library function for formatted output)
- Called from (representative examples):
  - [ExecMergeTupleDump](ExecMergeTupleDump.md) (general tuple dumping function that calls this for outer tuple display)

## Notes and Other Information
- This is a debug-only function typically compiled conditionally based on debug build settings
- Part of a suite of debugging utilities for merge join operations including similar functions for inner and marked tuples
- The output format includes a header "==== outer tuple ====" for clear identification in debug logs
- Used primarily during PostgreSQL development and debugging rather than in production environments
- The function assumes the mergestate parameter is valid and contains a properly initialized outer tuple slot

## Simplified Source

```c
static void
ExecMergeTupleDumpOuter(MergeJoinState *mergestate)
{
    // Get the outer tuple slot from merge join state
    TupleTableSlot *outerSlot = mergestate->mj_OuterTupleSlot;

    // Print debug header for outer tuple
    printf("==== outer tuple ====\n");

    // Print tuple contents or "(nil)" if empty
    if (TupIsNull(outerSlot))
        printf("(nil)\n");
    else
        MJ_debugtup(outerSlot);
}
```