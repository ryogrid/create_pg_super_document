# ExecReScanTidScan

## Location
src/backend/executor/nodeTidscan.c: 447 - 469

## Overview
ExecReScanTidScan resets a TID scan operation to its initial state, allowing the scan to be restarted from the beginning.

## Definition
```c
void
ExecReScanTidScan(TidScanState *node)
```

## Detailed Description
ExecReScanTidScan performs a rescan operation on a TID scan node, resetting all scan-related state to allow the scan to restart from the beginning. The function cleans up the current TID list by freeing any allocated memory, resets the TID pointer to -1 (indicating no current position), and clears the number of TIDs. If a current scan descriptor exists, it performs a table rescan to reset the underlying table scan state. Finally, it calls ExecScanReScan to handle any generic scan state reset operations.

## Parameters / Member Variables
- `node`: TidScanState pointer containing the TID scan execution state that needs to be reset

## Dependencies
- Functions called/Symbols referenced:
  - [pfree](../p/pfree.md) (to free the TID list memory)
  - [table_rescan](../t/table_rescan.md) (to reset the table scan descriptor)
  - [ExecScanReScan](ExecScanReScan.md) (generic scan state reset function)
- Called from (representative examples):
  - [ExecReScan](ExecReScan.md) (generic executor rescan dispatch function)
  - NODETIDSCAN_H (header file declaration)

## Notes and Other Information
- Properly cleans up dynamically allocated TID list memory to prevent memory leaks
- Resets tss_TidPtr to -1 to indicate no current scan position
- The table_rescan call with NULL parameter indicates a full rescan without new scan keys
- Follows the standard PostgreSQL executor pattern of calling generic rescan functions after node-specific cleanup
- The function ensures the scan can be restarted multiple times during query execution