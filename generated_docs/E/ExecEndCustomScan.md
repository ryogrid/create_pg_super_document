# ExecEndCustomScan

## Location
src/backend/executor/nodeCustom.c: 125 - 131

## Overview
Terminates execution of a Custom Scan node by calling the custom scan provider's cleanup callback.

## Definition
```c
void ExecEndCustomScan(CustomScanState *node)
```

## Detailed Description
ExecEndCustomScan is the cleanup function for custom scan nodes that is called when the executor shuts down or when the custom scan node is no longer needed. It delegates to the custom scan provider's EndCustomScan method to perform provider-specific cleanup operations such as releasing resources, closing connections, or freeing memory. This function is part of the standard executor node lifecycle.

## Parameters / Member Variables
- `node`: The CustomScanState node to be cleaned up

## Dependencies
- Functions called/Symbols referenced:
  - EndCustomScan (via node->methods callback)
- Called from (representative examples):
  - ExecEndNode

## Notes and Other Information
- This function only delegates to the custom scan provider's cleanup method
- The custom scan provider is responsible for implementing proper resource cleanup
- The function assumes the custom scan provider has properly implemented the EndCustomScan method
- Part of the standard executor node lifecycle along with ExecInitCustomScan and ExecCustomScan