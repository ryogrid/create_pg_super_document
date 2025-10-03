# ExecEndSampleScan

## Location
[src/backend/executor/nodeSamplescan.c:179-201](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeSamplescan.c#L179-L201)

## Overview
ExecEndSampleScan performs cleanup operations for a sample scan node, notifying the table sampling method that scanning is complete and closing any open table scan descriptors.

## Definition

```c
void
ExecEndSampleScan(SampleScanState *node)
```
## Detailed Description
ExecEndSampleScan is the cleanup function for sample scan executor nodes in PostgreSQL. It performs orderly shutdown operations by first calling the table sampling method's EndSampleScan function (if provided) to allow the sampling method to perform any necessary cleanup operations, such as releasing method-specific resources or updating statistics. After notifying the sampling method, it closes any open table scan descriptor that was used for accessing the underlying relation. This function ensures proper resource cleanup and follows PostgreSQL's executor node lifecycle pattern where each node type provides initialization, execution, and cleanup phases.

## Parameters / Member Variables
- `*node`: A pointer to the SampleScanState structure containing the sample scan's state and resources to be cleaned up
## Dependencies
- Functions called/Symbols referenced:
  - [table_endscan](../t/table_endscan.md)
  - [SampleScanState](../S/SampleScanState.md) (type reference)
  - EndSampleScan (via tsmroutine function pointer)
- Called from (representative examples):
  - [ExecEndNode](ExecEndNode.md)

## Notes and Other Information
- This is a void function that does not return any value
- Part of the standard executor node lifecycle (Init, Exec, End)
- Checks if EndSampleScan callback exists before calling it, as some sampling methods may not require cleanup
- Only closes the table scan descriptor if one was actually opened (ss_currentScanDesc is not NULL)
- Ensures proper resource deallocation to prevent memory leaks and resource exhaustion
- Called automatically by the executor infrastructure when a query completes or is aborted

## Simplified Source
```c
void ExecEndSampleScan(SampleScanState *node) {
    // Notify sampling method that scan is complete
    if (node->tsmroutine->EndSampleScan)
        node->tsmroutine->EndSampleScan(node);

    // Close the table scan descriptor if it was opened
    if (node->ss.ss_currentScanDesc)
        table_endscan(node->ss.ss_currentScanDesc);
}
```