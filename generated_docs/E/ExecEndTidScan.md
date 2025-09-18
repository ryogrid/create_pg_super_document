# ExecEndTidScan

## Location
src/backend/executor/nodeTidscan.c: 470 - 487

## Overview
ExecEndTidScan performs cleanup operations for a TID scan node, releasing any storage allocated during the scan operation.

## Definition
```c
void
ExecEndTidScan(TidScanState *node)
```

## Detailed Description
ExecEndTidScan is responsible for cleaning up resources associated with a TID scan operation when the scan is being terminated. The function checks if there is an active scan descriptor and, if so, calls table_endscan to properly close the table scan and release associated resources. This function is part of the standard PostgreSQL executor cleanup protocol, ensuring that all scan-related resources are properly freed when a TID scan node is finished executing.

## Parameters / Member Variables
- `node`: TidScanState pointer containing the TID scan execution state that needs cleanup

## Dependencies
- Functions called/Symbols referenced:
  - [table_endscan](../t/table_endscan.md) (to close and clean up the table scan descriptor)
- Called from (representative examples):
  - [ExecEndNode](ExecEndNode.md) (generic executor node cleanup dispatch function)
  - NODETIDSCAN_H (header file declaration)

## Notes and Other Information
- Only performs cleanup if there is an active scan descriptor (ss_currentScanDesc)
- Does not explicitly free the TID list memory (this may be handled elsewhere or during rescan operations)
- Follows the standard PostgreSQL executor pattern for node cleanup functions
- Essential for preventing resource leaks when TID scan operations complete
- The function is called during query plan tree teardown to ensure proper resource management