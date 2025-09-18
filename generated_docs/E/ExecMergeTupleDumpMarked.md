# ExecMergeTupleDumpMarked

## Location
[src/backend/executor/nodeMergejoin.c:570-581](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeMergejoin.c#L570-L581)

## Overview
ExecMergeTupleDumpMarked is a debug utility function that prints the current marked tuple in a merge join operation to stdout for debugging purposes.

## Definition


## Detailed Description
ExecMergeTupleDumpMarked is a specialized debugging utility function within the merge join executor that provides formatted output of the currently marked tuple. The marked tuple represents a position in the inner relation that the merge join algorithm has "marked" for potential backtracking during the join process, particularly important for handling duplicate values in merge joins.

The function examines the marked tuple slot in the merge join state and prints either the tuple contents using MJ_debugtup or "(nil)" if no tuple is currently marked. This debugging capability is crucial for developers working on merge join algorithms, especially when dealing with complex scenarios involving duplicate join keys where the algorithm needs to backtrack to previously processed positions.

## Parameters / Member Variables
- : MergeJoinState structure containing the merge join execution state, specifically accessing the marked tuple slot used for backtracking

## Dependencies
- Functions called/Symbols referenced:
  - [MergeJoinState](../M/MergeJoinState.md) (merge join state structure)
  - TupIsNull (macro to check if tuple slot is null)
  - MJ_debugtup (debug macro to print tuple contents)
  - printf (standard C library function for formatted output)
- Called from (representative examples):
  - ExecMergeTupleDump (general tuple dumping function that calls this for marked tuple display)

## Notes and Other Information
- This is a debug-only function typically compiled conditionally based on debug build settings
- Part of a complete debugging suite for merge join operations including functions for outer and inner tuples
- The output format includes a header "==== marked tuple ====" to clearly distinguish it from other tuple types in debug logs
- The marked tuple mechanism is essential for merge join's ability to handle duplicate values by allowing backtracking to previously processed positions
- Used primarily during PostgreSQL development and debugging rather than in production environments
- The function assumes the mergestate parameter is valid and contains a properly initialized marked tuple slot
- Particularly useful when debugging complex merge join scenarios involving duplicate join keys or outer join processing