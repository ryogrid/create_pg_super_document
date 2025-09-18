# ExecReScanForeignScan

## Location
src/backend/executor/nodeForeignscan.c: 323 - 355

## Overview
ExecReScanForeignScan rescans a foreign table relation by reinitializing the foreign scan state and restarting the scan from the beginning.

## Definition
void ExecReScanForeignScan(ForeignScanState *node)

## Detailed Description
This function performs a rescan operation on a foreign table by calling the foreign data wrapper's ReScanForeignScan routine. It handles special cases for EvalPlanQual (EPQ) processing by ignoring direct modifications (INSERT/UPDATE/DELETE) when EPQ is active, since these are irrelevant for EPQ rechecking. The function also manages rescanning of outer plans when necessary and updates the scan state.

## Parameters / Member Variables
- : Pointer to the ForeignScanState containing the execution state for the foreign scan operation

## Dependencies
- Functions called/Symbols referenced:
  - outerPlanState
  - [ExecReScan](ExecReScan.md)
  - [ExecScanReScan](ExecScanReScan.md)
  - [ForeignScanState](../F/ForeignScanState.md).fdwroutine->ReScanForeignScan
- Called from (representative examples):
  - [ExecReScan](ExecReScan.md)

## Notes and Other Information
- The function checks for EvalPlanQual activity and skips non-SELECT operations during EPQ processing
- Outer plans are only rescanned if they exist and their chgParam is NULL
- The actual foreign table rescan is delegated to the foreign data wrapper's ReScanForeignScan routine
- Located in src/backend/executor/nodeForeignscan.c:323-355