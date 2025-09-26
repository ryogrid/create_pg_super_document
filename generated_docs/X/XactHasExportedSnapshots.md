# XactHasExportedSnapshots

## Location
[src/backend/utils/time/snapmgr.c:1554-1566](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/time/snapmgr.c#L1554-L1566)

## Overview
XactHasExportedSnapshots checks whether the current transaction has exported any snapshots that are available for other transactions to import.

## Definition
```c
bool XactHasExportedSnapshots(void)
```

## Detailed Description
This is a simple utility function that determines if the current transaction has any active exported snapshots. It checks the global `exportedSnapshots` list, which maintains a record of all snapshots that have been exported by the current transaction via the `ExportSnapshot()` function.

The function provides a quick way to determine transaction state regarding snapshot exports, which is important for:
- Transaction cleanup and resource management
- Determining transaction commit/abort behavior
- Ensuring proper snapshot lifecycle management

## Parameters / Member Variables
- No parameters (void function)

## Dependencies
- Functions called/Symbols referenced:
  - exportedSnapshots (static List variable)
  - NIL (empty list constant)
- Called from (representative examples):
  - PrepareTransaction (to check if transaction has exported snapshots during prepare)

## Notes and Other Information
- Returns true if the transaction has exported one or more snapshots, false otherwise
- The `exportedSnapshots` is a static module-level List that tracks exported snapshots for the current transaction
- Used primarily for transaction state management and cleanup operations
- Simple boolean check with no side effects or complex logic