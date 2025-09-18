# table_endscan

## Location
src/include/access/tableam.h: 1020 - 1028

## Overview
Terminates a table scan operation by calling the table access method's scan_end function to clean up scan resources and release any locks or state.

## Definition


## Detailed Description
The  function serves as the standard way to terminate any table scan operation in PostgreSQL. It acts as a wrapper around the table access method's scan_end function, providing a uniform interface for ending scans regardless of the underlying storage implementation. This function is responsible for cleaning up scan-related resources, releasing locks, freeing memory, and performing any other cleanup operations required by the specific table access method.

The function is part of PostgreSQL's table access method (TAM) abstraction layer, which allows different storage engines to implement their own scan termination logic while providing a consistent interface to the rest of the system.

## Parameters / Member Variables
- : The TableScanDesc structure representing the scan to be terminated

## Dependencies
- Functions called/Symbols referenced:
  - TableScanDesc (scan descriptor type)
  - scan->rs_rd->rd_tableam->scan_end (table access method scan termination function)
- Called from (representative examples):
  - systable_endscan (src/backend/access/index/genam.c:612)
  - acquire_sample_rows (src/backend/commands/analyze.c:1264)
  - ExecEndSeqScan (src/backend/executor/nodeSeqscan.c:197)
  - ExecEndBitmapHeapScan (src/backend/executor/nodeBitmapHeapscan.c:674)

## Notes and Other Information
- This is an inline function defined in the table access method header file
- Must be called for every table scan that was started with table_beginscan or related functions
- Failure to call this function can result in resource leaks and potential deadlocks
- The function delegates to the specific table access method's scan_end implementation
- Used extensively throughout PostgreSQL for cleaning up various types of scans including sequential scans, bitmap scans, and analyze scans
- Part of the TAM abstraction that enables pluggable storage engines