# ExecReScanTableFuncScan

## Location
src/backend/executor/nodeTableFuncscan.c: 237 - 267

## Overview
This function rescans a table function scan node by clearing cached results and resetting the scan state when parameters change.

## Definition
```c
void ExecReScanTableFuncScan(TableFuncScanState *node)
```

## Detailed Description
ExecReScanTableFuncScan is responsible for rescanning a TableFuncScan execution node. The function handles parameter changes that may affect the table function output by clearing cached tuple store data and resetting the scan state. When parameters change (indicated by chgparam), the function destroys the existing tupstore to force recomputation on the next scan. If no parameters changed but a tupstore exists, it simply resets the tupstore position to the beginning.

## Parameters / Member Variables
- `node`: TableFuncScanState pointer representing the table function scan execution state

## Dependencies
- Functions called/Symbols referenced:
  - ExecClearTuple
  - ExecScanReScan
  - tuplestore_end
  - tuplestore_rescan
- Called from (representative examples):
  - ExecReScan
  - NODETABLEFUNCSCAN_H

## Notes and Other Information
- Part of PostgreSQL's executor framework for table function scanning
- Handles parameter change optimization by selectively clearing cached data
- Maintains tuple store state for efficient rescanning when parameters are unchanged
- Essential for proper execution of table functions with changing parameters