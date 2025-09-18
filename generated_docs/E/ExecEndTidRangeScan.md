# ExecEndTidRangeScan

## Location
src/backend/executor/nodeTidrangescan.c: 327 - 346

## Overview
ExecEndTidRangeScan performs cleanup operations for a TID range scan, releasing any storage allocated through C routines and properly terminating the scan.

## Definition
```c
void ExecEndTidRangeScan(TidRangeScanState *node)
```

## Detailed Description
ExecEndTidRangeScan is responsible for the cleanup and termination of a TID range scan operation. The function releases any resources that were allocated during the scan's lifecycle, specifically focusing on the table scan descriptor. It checks if there is an active scan descriptor and, if present, properly terminates it using the table_endscan function. This ensures that any locks, buffers, or other resources associated with the scan are properly released.

The function follows PostgreSQL's standard pattern for executor cleanup routines, ensuring that no resources are leaked when a plan node finishes execution or is terminated prematurely.

## Parameters / Member Variables
- `node`: A TidRangeScanState pointer containing the execution state for the TID range scan that needs to be cleaned up

## Dependencies
- Functions called/Symbols referenced:
  - [table_endscan](../t/table_endscan.md) (terminates the table scan and releases resources)
  - [TableScanDesc](../T/TableScanDesc.md) (table scan descriptor type)
- Called from (representative examples):
  - [ExecEndNode](ExecEndNode.md) (generic plan node termination dispatcher)

## Notes and Other Information
- The function safely handles the case where no scan descriptor exists (scan == NULL)
- This is part of the standard PostgreSQL executor cleanup interface
- Proper resource cleanup is critical to prevent memory leaks and lock contention
- The function assumes that any other cleanup specific to TID range scans has already been handled elsewhere, focusing only on the generic scan descriptor cleanup