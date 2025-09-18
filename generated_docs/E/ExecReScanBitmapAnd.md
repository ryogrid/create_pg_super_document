# ExecReScanBitmapAnd

## Location
src/backend/executor/nodeBitmapAnd.c: 201 - 223

## Overview
ExecReScanBitmapAnd handles parameter changes and rescan operations for BitmapAndState nodes by propagating parameter updates and rescanning all child subplans.

## Definition


## Detailed Description
ExecReScanBitmapAnd manages the rescan process for BitmapAnd executor nodes, which is necessary when query parameters change during execution (such as in nested loops). The function iterates through all subplan nodes and handles parameter change propagation and rescanning logic.

The function implements a two-phase approach to rescanning. First, it propagates any changed parameters from the current node to each subplan using UpdateChangedParamSet, ensuring that subplans are aware of parameter changes that affect them. Then, for subplans that don't have pending parameter changes, it immediately calls ExecReScan to reset their execution state.

The parameter change handling follows PostgreSQL's lazy rescanning optimization - if a subplan has pending parameter changes (chgParam is not NULL), it will automatically be rescanned on its first subsequent execution call, so an explicit rescan is unnecessary and avoided for efficiency.

## Parameters / Member Variables
- : Pointer to the BitmapAndState containing the subplans to be rescanned

## Dependencies
- Functions called/Symbols referenced:
  - [UpdateChangedParamSet](../U/UpdateChangedParamSet.md) (to propagate parameter changes to subplans)
  - [ExecReScan](ExecReScan.md) (to rescan subplans without pending parameter changes)
- Called from (representative examples):
  - [ExecReScan](ExecReScan.md) (general rescan dispatcher)

## Notes and Other Information
- Part of PostgreSQL's parameter change propagation and rescan infrastructure
- Implements lazy rescanning optimization for efficiency
- Handles the complexity of parameter-dependent subplan rescanning
- Essential for correct behavior in nested loop joins and parameterized plans
- Uses UpdateChangedParamSet, which was mentioned in the provided context as filtering parameter changes to only those the node actually depends on
- Located in src/backend/executor/nodeBitmapAnd.c:201-223