# table_rescan

## Location
[src/include/access/tableam.h:1029-1043](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/tableam.h#L1029-L1043)

## Overview
Restarts a table scan operation from the beginning, optionally applying new scan keys to filter the results during the rescan.

## Definition

```c
static inline void
table_rescan(TableScanDesc scan,
			 struct ScanKeyData *key)
```
## Detailed Description
The  function provides a way to restart an existing table scan from the beginning without having to end the current scan and start a new one. This is more efficient than terminating and reinitializing a scan, as it can reuse existing scan state and resources. The function allows for optionally specifying new scan keys that will be used to filter rows during the rescan.

This function is commonly used in executor nodes that need to rescan their input, such as nested loop joins where the inner scan needs to be repeated for each outer tuple. The function delegates to the table access method's scan_rescan implementation with default parameters (all boolean flags set to false).

## Parameters / Member Variables
- `scan`: The TableScanDesc structure representing the scan to be restarted
- `*key`: Optional new scan key data to apply during the rescan (can be NULL)
## Dependencies
- Functions called/Symbols referenced:
  - [TableScanDesc](../T/TableScanDesc.md) (scan descriptor type)
  - [ScanKeyData](../S/ScanKeyData.md) (scan key structure type)
  - scan->rs_rd->rd_tableam->scan_rescan (table access method rescan function)
- Called from (representative examples):
  - [ExecReScanSeqScan](../E/ExecReScanSeqScan.md) (src/backend/executor/nodeSeqscan.c:219)
  - [ExecReScanBitmapHeapScan](../E/ExecReScanBitmapHeapScan.md) (src/backend/executor/nodeBitmapHeapscan.c:601)
  - [ExecReScanTidScan](../E/ExecReScanTidScan.md) (src/backend/executor/nodeTidscan.c:457)
  - [RelationFindReplTupleSeq](../R/RelationFindReplTupleSeq.md) (src/backend/executor/execReplication.c:401)

## Notes and Other Information
- This is an inline function defined in the table access method header file
- More efficient than ending and restarting a scan as it preserves scan context
- Commonly used in nested loop operations and other scenarios requiring multiple passes
- The function calls scan_rescan with all boolean parameters set to false (default behavior)
- Part of PostgreSQL's table access method (TAM) abstraction layer
- Can change scan keys mid-scan, allowing for dynamic filtering during rescans
- Used extensively in executor nodes that implement rescan functionality

## Simplified Source

```c
static inline void table_rescan(TableScanDesc scan, struct ScanKeyData *key) {
    // Delegate to table access method's rescan implementation
    scan->rs_rd->rd_tableam->scan_rescan(scan, key, false, false, false, false);
}
```