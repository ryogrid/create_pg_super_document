# ExecEndSeqScan

## Location
[src/backend/executor/nodeSeqscan.c:184-211](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeSeqscan.c#L184-L211)

## Overview
ExecEndSeqScan performs cleanup operations for a sequential scan node by closing the table scan and freeing associated resources.

## Definition
```c
void ExecEndSeqScan(SeqScanState *node)
```

## Detailed Description
ExecEndSeqScan handles the termination and cleanup of a sequential scan operation. Its primary responsibility is to properly close the table scan descriptor if one exists, ensuring that any resources allocated during the scan are properly freed. The function retrieves the current scan descriptor from the node state and calls table_endscan to perform the actual cleanup. This follows PostgreSQL's resource management pattern where each initialization function has a corresponding cleanup function.

## Parameters / Member Variables
- `node`: SeqScanState pointer containing the scan state with the scan descriptor to be cleaned up

## Dependencies
- Functions called/Symbols referenced:
  - [table_endscan](../t/table_endscan.md)
  - [SeqScanState](../S/SeqScanState.md)
  - [TableScanDesc](../T/TableScanDesc.md)
- Called from (representative examples):
  - [ExecEndNode](ExecEndNode.md)
  - NODESEQSCAN_H

## Notes and Other Information
- Returns void as it performs cleanup operations only
- Includes a null check before calling table_endscan to handle cases where no scan was initiated
- Part of PostgreSQL's resource management lifecycle (Init -> Exec -> End)
- Essential for preventing resource leaks in long-running transactions
- The function is minimal by design, focusing only on scan-specific cleanup
- Other cleanup operations (like expression context and tuple slots) are handled by higher-level cleanup functions

## Simplified Source
```c
void ExecEndSeqScan(SeqScanState *node) {
    // Get the scan descriptor from node state
    TableScanDesc scanDesc = node->ss.ss_currentScanDesc;

    // Close the table scan if it was opened
    if (scanDesc != NULL)
        table_endscan(scanDesc);
}
```