# ExecEndMergeAppend

## Location
[src/backend/executor/nodeMergeAppend.c:320-339](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeMergeAppend.c#L320-L339)

## Overview
Shuts down a MergeAppend node by recursively calling ExecEndNode on all of its child subplans to ensure proper cleanup and resource deallocation.

## Definition
```c
void ExecEndMergeAppend(MergeAppendState *node)
```

## Detailed Description
ExecEndMergeAppend is the cleanup function for MergeAppend executor nodes, responsible for orderly shutdown of the merge operation and its constituent parts. The function implements a simple but essential cleanup pattern:

1. **State Extraction**: Retrieves the array of child plan states and the count of subplans from the MergeAppendState
2. **Recursive Cleanup**: Iterates through all initialized subplans and calls ExecEndNode on each one
3. **Resource Management**: Ensures that all child plans are properly terminated, allowing them to release memory, close files, and perform any necessary cleanup

The function follows PostgreSQL's standard executor cleanup protocol where each node is responsible for cleaning up its children before being cleaned up itself. This creates a clean shutdown cascade from parent to child nodes throughout the execution tree.

Note that the function only handles cleanup of the subplans themselves - the MergeAppendState structure and its associated memory (like the binary heap, tuple slots array, and sort keys) are cleaned up by the broader executor framework when the memory context is destroyed.

## Parameters / Member Variables
- `*node`: The MergeAppendState containing the merge execution state and array of child plan states to be shut down
## Dependencies
- Functions called/Symbols referenced:
  - [ExecEndNode](ExecEndNode.md) (recursively shuts down each child plan)
- Called from (representative examples):
  - [ExecEndNode](ExecEndNode.md) (main executor cleanup dispatcher)

## Notes and Other Information
- The function is part of the standard PostgreSQL executor node lifecycle (Init -> Exec -> End)
- Does not return any value (void function) as cleanup operations typically don't have meaningful return values
- The cleanup is performed regardless of whether all subplans were actually executed (e.g., due to LIMIT or early termination)
- Memory allocated for the MergeAppendState itself and related structures is handled by memory context destruction rather than explicit deallocation
- Critical for preventing resource leaks in complex queries with multiple subplans
- The function assumes that all entries in the mergeplans array are valid PlanState pointers (non-null)

## Simplified Source

```c
void ExecEndMergeAppend(MergeAppendState *node) {
    // Extract the array of child plans and count
    PlanState **mergeplans = node->mergeplans;
    int nplans = node->ms_nplans;

    // Recursively shut down each child plan
    for (int i = 0; i < nplans; i++)
        ExecEndNode(mergeplans[i]);
}
```