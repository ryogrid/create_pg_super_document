# ExecAssignScanProjectionInfoWithVarno

## Location
src/backend/executor/execScan.c: 283 - 296

## Overview
ExecAssignScanProjectionInfoWithVarno is a variant of ExecAssignScanProjectionInfo that allows the caller to explicitly specify the varno (variable number) expected in Vars within the target list.

## Definition


## Detailed Description
ExecAssignScanProjectionInfoWithVarno provides flexibility for scan nodes that need to specify a custom varno when setting up projection information. This function is particularly useful for specialized scan types like foreign scans, custom scans, and index-only scans where the standard scanrelid from the plan might not be appropriate for projection analysis.

Like its counterpart ExecAssignScanProjectionInfo, this function delegates the actual projection setup to ExecConditionalAssignProjectionInfo, but allows the caller to override the varno parameter rather than extracting it from the scan plan's scanrelid field. This enables more precise control over how variable references in the target list are matched against the tuple descriptor.

## Parameters / Member Variables
- : The ScanState containing the scan tuple slot with descriptor information
- : The variable number to be used when analyzing Vars in the target list for projection optimization

## Dependencies
- Functions called/Symbols referenced:
  - ExecConditionalAssignProjectionInfo (performs projection analysis with the specified varno)
- Data structures used:
  - ScanState
  - TupleDesc (from scan tuple slot)
- Called from (representative examples):
  - ExecInitForeignScan (foreign scan initialization)
  - ExecInitCustomScan (custom scan initialization)
  - ExecInitIndexOnlyScan (index-only scan initialization)

## Notes and Other Information
- This function provides the same projection optimization as ExecAssignScanProjectionInfo but with explicit varno control
- Primarily used by scan types that don't follow the standard scanrelid pattern or need custom variable numbering
- The scan slot's tuple descriptor must be properly set before calling this function
- Foreign scans and custom scans often use this variant because they may have complex variable mapping requirements
- Index-only scans use this function because their variable references may not align with the standard scan relation ID
- The varno parameter affects how variable references in the target list are matched during projection optimization analysis