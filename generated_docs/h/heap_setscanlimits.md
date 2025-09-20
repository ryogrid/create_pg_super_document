# heap_setscanlimits

## Location
[src/backend/access/heap/heapam.c:466-487](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heapam.c#L466-L487)

## Overview
Sets the scan range limits for a heap table scan by specifying the starting block and number of blocks to scan.

## Definition

```c
void
heap_setscanlimits(TableScanDesc sscan, BlockNumber startBlk, BlockNumber numBlks)
```
## Detailed Description
heap_setscanlimits restricts the range of a heap table scan to a specific subset of blocks. This function is used to limit scanning to a particular range of pages within a table, which is useful for operations like TID range scans, index builds on specific ranges, or other scenarios where only a portion of the table needs to be scanned.

The function validates that:
1. The scan has not been initialized yet (rs_inited must be false)
2. Synchronized scanning is not enabled (SO_ALLOW_SYNC flag must not be set)
3. The starting block number is valid (either 0 or within the table's block range)

Once set, the scan will begin at startBlk and process numBlks blocks. If numBlks is InvalidBlockNumber, it means scan all blocks from the starting position to the end of the table.

## Parameters / Member Variables
- : TableScanDesc (cast to HeapScanDesc internally) representing the scan descriptor to modify
- : BlockNumber specifying the first block number to scan (0-based)
- : BlockNumber specifying how many blocks to scan, or InvalidBlockNumber to scan all remaining blocks

## Dependencies
- Functions called/Symbols referenced:
  - [HeapScanDesc](../H/HeapScanDesc.md) (type cast)
  - SO_ALLOW_SYNC (flag check)
- Called from (representative examples):
  - [heap_set_tidrange](heap_set_tidrange.md) (src/backend/access/heap/heapam.c:1421, 1440)
  - [heapam_index_build_range_scan](heapam_index_build_range_scan.md) (src/backend/access/heap/heapam_handler.c:1319)
  - HeapScanIsValid (src/include/access/heapam.h:293)

## Notes and Other Information
- Must be called before scan initialization (rs_inited must be false)
- Cannot be used with synchronized scans (SO_ALLOW_SYNC must not be set)
- Primarily used for TID range scans where only specific page ranges need to be processed
- The function performs boundary checking to ensure startBlk is within valid range
- Setting numBlks to InvalidBlockNumber means "scan from startBlk to end of table"
- Used by index build operations to scan specific ranges of a table during parallel index creation
- The scan limits override the default behavior of scanning the entire table