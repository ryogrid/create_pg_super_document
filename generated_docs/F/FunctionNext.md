# FunctionNext

## Location
src/backend/executor/nodeFunctionscan.c: 59 - 248

## Overview
FunctionNext is the core workhorse function for ExecFunctionScan that retrieves the next tuple from table functions in a function scan operation.

## Definition


## Detailed Description
FunctionNext implements the tuple retrieval logic for function scans in PostgreSQL's executor. It handles both simple and complex function scan scenarios:

1. **Simple Path**: When the function return type matches the scan result type, it directly fetches results into the scan slot for optimal performance.

2. **Complex Path**: For multiple functions or type mismatches, it:
   - Manages ordinal counters for positioning
   - Iterates through all functions in the function list
   - Copies values from function slots to the scan slot
   - Handles NULL padding for functions that return fewer rows
   - Adds ordinality columns when requested

The function uses tuplestores to cache function results, allowing for efficient forward and backward scanning. It properly handles end-of-data conditions and maintains row count information for backward scan support.

## Parameters / Member Variables
- : FunctionScanState containing the scan state information, function states, tuple slots, and execution flags

## Dependencies
- Functions called/Symbols referenced:
  - ExecMakeTableFunctionResult
  - tuplestore_rescan
  - tuplestore_gettupleslot
  - ExecClearTuple
  - slot_getallattrs
  - ExecStoreVirtualTuple
  - ScanDirectionIsForward
  - TupIsNull
  - Int64GetDatumFast
- Called from (representative examples):
  - ExecFunctionScan

## Notes and Other Information
- Implements both forward and backward scanning capabilities
- Uses tuplestore caching to avoid re-executing functions on subsequent calls
- Handles mixed row counts from multiple functions by padding shorter results with NULLs
- Maintains accurate ordinal positioning for ORDINALITY columns
- Optimizes the common single-function case with a fast path
- Supports the EXEC_FLAG_BACKWARD execution flag for backward-compatible scans