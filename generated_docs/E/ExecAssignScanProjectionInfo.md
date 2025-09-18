# ExecAssignScanProjectionInfo

## Location
src/backend/executor/execScan.c: 270 - 282

## Overview
ExecAssignScanProjectionInfo sets up projection information for scan nodes by determining whether projection is needed based on target list and tuple descriptor matching.

## Definition


## Detailed Description
ExecAssignScanProjectionInfo is a specialized wrapper function that configures projection information for scan operations. It extracts the scan plan and tuple descriptor from the scan node, then delegates to ExecConditionalAssignProjectionInfo to determine if projection is necessary.

The function implements a key optimization in PostgreSQL's execution engine: avoiding unnecessary projection steps when the requested target list exactly matches the underlying tuple structure. This optimization is common not only in simple "SELECT *" queries but also in complex queries where the planner generates matching target lists for joined or processed nodes above the scan.

## Parameters / Member Variables
- : The ScanState containing the scan plan and tuple slot with descriptor information

## Dependencies
- Functions called/Symbols referenced:
  - [ExecConditionalAssignProjectionInfo](ExecConditionalAssignProjectionInfo.md) (performs the actual projection analysis and setup)
- Data structures used:
  - [ScanState](../S/ScanState.md)
  - Scan (plan node)
  - [TupleDesc](../T/TupleDesc.md) (from scan tuple slot)
- Called from (representative examples):
  - [ExecInitSeqScan](ExecInitSeqScan.md) (sequential scan initialization)
  - [ExecInitIndexScan](ExecInitIndexScan.md) (index scan initialization)
  - [ExecInitBitmapHeapScan](ExecInitBitmapHeapScan.md) (bitmap heap scan initialization)
  - [ExecInitFunctionScan](ExecInitFunctionScan.md) (function scan initialization)
  - All other scan node initialization functions

## Notes and Other Information
- This function is a thin wrapper that provides scan-specific parameter extraction for the generic ExecConditionalAssignProjectionInfo
- The scan slot's tuple descriptor must be properly set before calling this function
- Projection optimization helps avoid unnecessary tuple copying and transformation when input and output formats match
- The scanrelid parameter passed to ExecConditionalAssignProjectionInfo helps with relation-specific projection decisions
- This function is typically called during scan node initialization rather than during execution