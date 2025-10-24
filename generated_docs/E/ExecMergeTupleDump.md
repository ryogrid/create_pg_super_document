# ExecMergeTupleDump

## Location
[src/backend/executor/nodeMergejoin.c:582-598](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeMergejoin.c#L582-L598)

## Overview
A debugging utility function that prints detailed information about all tuples currently held by a merge join state, including outer, inner, and marked tuples.

## Definition
```c
static void ExecMergeTupleDump(MergeJoinState *mergestate)
```

## Detailed Description
ExecMergeTupleDump is a static debugging function designed to provide comprehensive visibility into the current state of tuple slots within a merge join operation. This function serves as a debugging aid by displaying the contents of all three critical tuple slots maintained by the merge join: the outer tuple slot, inner tuple slot, and marked tuple slot. The function outputs formatted debugging information to stdout, making it invaluable during development and troubleshooting of merge join operations.

The function operates by sequentially calling three specialized dump functions, each responsible for displaying the contents of a specific tuple slot. The output is formatted with clear headers and delimiters to distinguish between different tuple types and provide a readable debug trace.

## Parameters / Member Variables
- `mergestate`: Pointer to the MergeJoinState structure containing the current state of the merge join operation, including all tuple slots to be dumped

## Dependencies
- Functions called/Symbols referenced:
  - [ExecMergeTupleDumpOuter](ExecMergeTupleDumpOuter.md)
  - [ExecMergeTupleDumpInner](ExecMergeTupleDumpInner.md)  
  - [ExecMergeTupleDumpMarked](ExecMergeTupleDumpMarked.md)
  - [MergeJoinState](../M/MergeJoinState.md) (type)
- Called from (representative examples):
  - MJ_dump (macro in execdebug.h)

## Notes and Other Information
- This function is conditionally compiled and typically only available in debug builds
- Used primarily for debugging merge join algorithm behavior and state transitions
- The function provides a complete snapshot of all tuple slots in the merge join state
- Output includes clear formatting with headers like "==== outer tuple ====" for easy identification
- Each helper function handles null tuple checking and appropriate display formatting
- The MJ_dump macro in execdebug.h provides a convenient interface to call this function

## Simplified Source

```c
static void
ExecMergeTupleDump(MergeJoinState *mergestate)
{
    // Print main debug header
    printf("******** ExecMergeTupleDump ********\n");

    // Dump all three tuple types for complete state visibility
    ExecMergeTupleDumpOuter(mergestate);   // Outer relation tuple
    ExecMergeTupleDumpInner(mergestate);   // Inner relation tuple
    ExecMergeTupleDumpMarked(mergestate);  // Marked position for backtracking

    // Print footer delimiter
    printf("********\n");
}
```