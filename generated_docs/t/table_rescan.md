# table_rescan

## Location
src/include/access/tableam.h: 1029 - 1043

## Overview
Restarts a table scan operation from the beginning, optionally applying new scan keys to filter the results during the rescan.

## Definition


## Detailed Description
The  function provides a way to restart an existing table scan from the beginning without having to end the current scan and start a new one. This is more efficient than terminating and reinitializing a scan, as it can reuse existing scan state and resources. The function allows for optionally specifying new scan keys that will be used to filter rows during the rescan.

This function is commonly used in executor nodes that need to rescan their input, such as nested loop joins where the inner scan needs to be repeated for each outer tuple. The function delegates to the table access method's scan_rescan implementation with default parameters (all boolean flags set to false).

## Parameters / Member Variables
- : The TableScanDesc structure representing the scan to be restarted
- : Optional new scan key data to apply during the rescan (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - TableScanDesc (scan descriptor type)
  - ScanKeyData (scan key structure type)
  - scan->rs_rd->rd_tableam->scan_rescan (table access method rescan function)
- Called from (representative examples):
  - ExecReScanSeqScan (src/backend/executor/nodeSeqscan.c:219)
  - ExecReScanBitmapHeapScan (src/backend/executor/nodeBitmapHeapscan.c:601)
  - ExecReScanTidScan (src/backend/executor/nodeTidscan.c:457)
  - RelationFindReplTupleSeq (src/backend/executor/execReplication.c:401)

## Notes and Other Information
- This is an inline function defined in the table access method header file
- More efficient than ending and restarting a scan as it preserves scan context
- Commonly used in nested loop operations and other scenarios requiring multiple passes
- The function calls scan_rescan with all boolean parameters set to false (default behavior)
- Part of PostgreSQL's table access method (TAM) abstraction layer
- Can change scan keys mid-scan, allowing for dynamic filtering during rescans
- Used extensively in executor nodes that implement rescan functionality