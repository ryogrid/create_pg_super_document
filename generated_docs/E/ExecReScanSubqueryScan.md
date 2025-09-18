# ExecReScanSubqueryScan

## Location
src/backend/executor/nodeSubqueryscan.c: 183 - 201

## Overview
Rescans a SubqueryScan node during query execution, handling parameter changes and coordinating the rescan operation with the underlying subplan.

## Definition
void ExecReScanSubqueryScan(SubqueryScanState *node)

## Detailed Description
This function implements the rescan operation for SubqueryScan executor nodes in PostgreSQL's execution engine. It performs a two-phase rescan process: first rescanning the scan state itself, then handling parameter propagation to the subplan and conditionally rescanning the subplan based on parameter changes.

The function is responsible for managing the coordination between the outer scan node and its subplan, ensuring that parameter changes are properly propagated and that unnecessary rescans are avoided when the subplan's parameters haven't changed.

## Parameters / Member Variables
- : Pointer to the SubqueryScanState structure representing the current subquery scan node state, containing both the scan state and the subplan reference

## Dependencies
- Functions called/Symbols referenced:
  - ExecScanReScan (rescans the base scan state)
  - UpdateChangedParamSet (propagates parameter changes to subplan)
  - ExecReScan (rescans the subplan if needed)
  - SubqueryScanState (the node state structure)
- Called from (representative examples):
  - ExecReScan (general rescan dispatcher in execAmi.c:205)

## Notes and Other Information
- The function handles parameter change signaling manually because the subplan operates in its own memory context where chgParam state is maintained separately
- Optimization: If the subplan's chgParam is NULL (no parameters changed), ExecReScan is called immediately; otherwise, the subplan will be automatically re-scanned on the first ExecProcNode call
- This is part of PostgreSQL's executor node interface, specifically for handling subquery scans within larger query plans
- The function is declared in src/include/executor/nodeSubqueryscan.h and implemented in src/backend/executor/nodeSubqueryscan.c:183-201