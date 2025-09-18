# ss_report_location

## Location
src/backend/access/common/syncscan.c: 289 - 323

## Overview
Updates the shared scan location state to inform other concurrent scans about the current progress of a sequential table scan.

## Definition
```c
void ss_report_location(Relation rel, BlockNumber location)
```

## Detailed Description
ss_report_location is responsible for maintaining the shared state that enables synchronized scanning across multiple concurrent sequential scans. As a scan progresses through a table, this function periodically updates the global scan location cache so that new scans can start near the current position of existing scans.

Key features of the implementation:
1. **Throttled Updates**: Only reports progress every SYNC_SCAN_REPORT_INTERVAL pages (128KB worth) to reduce lock contention
2. **Non-blocking**: Uses LWLockConditionalAcquire to avoid blocking if the lock is busy - missing occasional updates is acceptable
3. **Performance-focused**: Designed to minimize overhead on scan performance while still providing coordination benefits

The function balances between providing useful coordination information and avoiding performance degradation from frequent locking operations.

## Parameters / Member Variables
- `rel`: Pointer to the Relation structure representing the table being scanned
- `location`: The current BlockNumber position in the scan

## Dependencies
- Functions called/Symbols referenced:
  - LWLockConditionalAcquire
  - LWLockRelease
  - [ss_search](ss_search.md) (internal function with set=true)
  - SYNC_SCAN_REPORT_INTERVAL (constant, 128KB / BLCKSZ)
  - SyncScanLock (lock identifier)
- Called from (representative examples):
  - [heapgettup_advance_block](../h/heapgettup_advance_block.md)
  - [heapam_scan_sample_next_block](../h/heapam_scan_sample_next_block.md)
  - [table_block_parallelscan_nextpage](../t/table_block_parallelscan_nextpage.md)

## Notes and Other Information
- Updates are throttled to every SYNC_SCAN_REPORT_INTERVAL pages to balance coordination benefits with performance
- Uses conditional locking to avoid blocking - missed updates are acceptable for performance
- The reporting interval is designed to be smaller than the buffer ring size used for bulk reads
- Includes optional trace logging for debugging when TRACE_SYNCSCAN is enabled
- Critical component of PostgreSQL's synchronized scan optimization for reducing I/O in concurrent table scans
- The non-blocking approach ensures that synchronized scanning never degrades individual scan performance