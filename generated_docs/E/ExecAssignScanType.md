# ExecAssignScanType

## Location
src/backend/executor/execUtils.c: 647 - 658

## Overview
Assigns a tuple descriptor to a scan node's scan tuple slot, establishing the type information for tuples produced by the scan operation.

## Definition
```c
void ExecAssignScanType(ScanState *scanstate, TupleDesc tupDesc)
```

## Detailed Description
ExecAssignScanType is a utility function that configures the scan tuple slot of a scan node with a specific tuple descriptor. This function is part of the executor's scan node support infrastructure and is typically called during the initialization phase of scan operations to establish the schema and type information for the tuples that will be produced by the scan.

The function operates by taking the scan tuple slot from the provided ScanState and calling ExecSetSlotDescriptor to associate it with the given tuple descriptor. This establishes the metadata needed for proper tuple handling throughout the scan operation.

## Parameters / Member Variables
- `scanstate`: Pointer to the ScanState structure containing the scan node's execution state, including the scan tuple slot that needs type assignment
- `tupDesc`: TupleDesc structure describing the schema and type information for tuples that will be stored in the scan slot

## Dependencies
- Functions called/Symbols referenced:
  - ExecSetSlotDescriptor
  - ScanState (struct type)
- Called from (representative examples):
  - ExecWorkTableScan
  - ResetPerTupleExprContext

## Notes and Other Information
- This is a simple wrapper function that provides a clean interface for assigning tuple types to scan slots
- The function is part of the scan node support utilities in the executor
- Located in src/backend/executor/execUtils.c:647-658
- Essential for proper tuple slot initialization in various scan node types