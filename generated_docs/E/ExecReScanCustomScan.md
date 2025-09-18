# ExecReScanCustomScan

## Location
src/backend/executor/nodeCustom.c: 132 - 138

## Overview
Resets a Custom Scan node to restart scanning from the beginning by calling the custom scan provider's rescan callback.

## Definition
```c
void ExecReScanCustomScan(CustomScanState *node)
```

## Detailed Description
ExecReScanCustomScan is called when a custom scan node needs to be reset to restart scanning from the beginning. This typically occurs in nested loop joins where the inner scan needs to be restarted for each tuple from the outer scan, or when a plan node above in the tree needs to rescan its input. The function delegates to the custom scan provider's ReScanCustomScan method to perform provider-specific rescan operations such as resetting internal state, repositioning cursors, or reinitializing data structures.

## Parameters / Member Variables
- `node`: The CustomScanState node to be rescanned

## Dependencies
- Functions called/Symbols referenced:
  - ReScanCustomScan (via node->methods callback)
- Called from (representative examples):
  - ExecReScan

## Notes and Other Information
- This function is essential for proper execution of nested loop joins and other query patterns requiring multiple scans
- The custom scan provider must properly implement state reset logic in their ReScanCustomScan callback
- The function assumes the custom scan provider has implemented the ReScanCustomScan method
- Rescan operations must ensure the scan can be restarted from the beginning with consistent results