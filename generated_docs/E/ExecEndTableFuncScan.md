# ExecEndTableFuncScan

## Location
[src/backend/executor/nodeTableFuncscan.c:220-236](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeTableFuncscan.c#L220-L236)

## Overview
ExecEndTableFuncScan performs cleanup operations for a table function scan node, specifically releasing tuplestore resources allocated during execution.

## Definition

```c
void
ExecEndTableFuncScan(TableFuncScanState *node)
```
## Detailed Description
ExecEndTableFuncScan handles the cleanup phase of table function scan execution by releasing any tuplestore resources that were allocated during the scan. The function checks if a tuplestore exists and properly deallocates it using tuplestore_end(), then sets the pointer to NULL to prevent dangling references.

This cleanup is essential because table function scans cache all their results in a tuplestore during the first execution pass, and this memory must be properly released when the scan completes or is terminated.

## Parameters / Member Variables
- : TableFuncScanState structure containing the scan state, including the tuplestore to be cleaned up

## Dependencies
- Functions called/Symbols referenced:
  - [TableFuncScanState](../T/TableFuncScanState.md) (struct type)
  - [tuplestore_end](../t/tuplestore_end.md) (function to deallocate tuplestore resources)
- Called from:
  - [ExecEndNode](ExecEndNode.md) (main executor cleanup function)
  - Referenced in nodeTableFuncscan.h header

## Notes and Other Information
- Part of PostgreSQL's three-phase execution model (Init, Execute, End)
- Only deallocates tuplestore resources - other memory contexts are handled elsewhere
- The function safely handles cases where no tuplestore was created (e.g., if execution never began)
- Essential for preventing memory leaks in long-running queries or repeated table function calls

## Simplified Source

```c
void ExecEndTableFuncScan(TableFuncScanState *node) {
    // Release tuplestore resources if allocated
    if (node->tupstore != NULL) {
        tuplestore_end(node->tupstore);
        node->tupstore = NULL;
    }
}
```