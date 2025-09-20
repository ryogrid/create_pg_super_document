# index_rescan

## Location
[src/backend/access/index/indexam.c:352-377](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/index/indexam.c#L352-L377)

## Overview
The  function restarts an existing index scan with potentially new scan keys and ordering specifications, resetting the scan state while preserving the scan descriptor.

## Definition

```c
structure itself */
	IndexScanEnd(scan);
```
## Detailed Description
This function allows restarting an active index scan with new search parameters without requiring complete scan teardown and recreation. It validates that the new key counts match the original scan setup, releases any resources from previous table accesses, resets scan state flags, and delegates to the access method-specific rescan implementation. The function is more efficient than ending and restarting a scan when only the search conditions need to change. It maintains the same scan descriptor while allowing the underlying index access method to reinitialize its internal scan state with the new parameters.

## Parameters / Member Variables
- : The active IndexScanDesc to be restarted
- : Array of new scan keys (search conditions), can be NULL to keep existing keys
- : Number of scan keys (must match scan->numberOfKeys from original beginscan)
- : Array of new ordering keys, can be NULL to keep existing ordering  
- : Number of ordering keys (must match scan->numberOfOrderBys from original beginscan)

## Dependencies
- Functions called/Symbols referenced:
  - SCAN_CHECKS (macro for scan descriptor validation)
  - CHECK_SCAN_PROCEDURE (macro to verify amrescan procedure exists)  
  - table_index_fetch_reset (reset heap tuple fetching resources)
  - ScanKey (scan key structure type)
  - [IndexScanDesc](../I/IndexScanDesc.md) (scan descriptor structure type)
- Called from (representative examples):
  - [systable_beginscan](../s/systable_beginscan.md) (src/backend/access/index/genam.c:444)
  - [systable_beginscan_ordered](../s/systable_beginscan_ordered.md) (src/backend/access/index/genam.c:702)
  - [ExecReScanIndexScan](../E/ExecReScanIndexScan.md) (src/backend/executor/nodeIndexscan.c:585)
  - [ExecReScanBitmapIndexScan](../E/ExecReScanBitmapIndexScan.md) (src/backend/executor/nodeBitmapIndexscan.c:165)

## Notes and Other Information
- Key count constraints: nkeys and norderbys must exactly match the original beginscan parameters
- Passing NULL for keys/orderbys preserves existing search conditions from the original scan
- Resets scan state flags like kill_prior_tuple and xs_heap_continue for safety
- More efficient than ending and restarting scans when only search parameters change
- Commonly used in nested loop joins and parameter changes during query execution
- Located in src/backend/access/index/indexam.c:352-377