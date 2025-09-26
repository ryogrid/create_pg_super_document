# ExecReScanTableFuncScan

## Location
[src/backend/executor/nodeTableFuncscan.c:237-267](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeTableFuncscan.c#L237-L267)

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
  - [ExecClearTuple](ExecClearTuple.md)
  - [ExecScanReScan](ExecScanReScan.md)
  - [tuplestore_end](../t/tuplestore_end.md)
  - [tuplestore_rescan](../t/tuplestore_rescan.md)
- Called from (representative examples):
  - [ExecReScan](ExecReScan.md)
  - NODETABLEFUNCSCAN_H

## Notes and Other Information
- Part of PostgreSQL's executor framework for table function scanning
- Handles parameter change optimization by selectively clearing cached data
- Maintains tuple store state for efficient rescanning when parameters are unchanged
- Essential for proper execution of table functions with changing parameters