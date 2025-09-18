# read_local_xlog_page

## Location
src/backend/access/transam/xlogutils.c: 861 - 872

## Overview
A public XLogReaderRoutine page_read callback function for reading local WAL files, designed to be useful for applications outside of walsender such as background workers.

## Definition
```c
int read_local_xlog_page(XLogReaderState *state, XLogRecPtr targetPagePtr,
                        int reqLen, XLogRecPtr targetRecPtr, char *cur_page)
```

## Detailed Description
This function serves as a public interface to the WAL page reading mechanism for local WAL files. It acts as a simple wrapper around read_local_xlog_page_guts, providing the standard page_read callback interface expected by XLogReaderState.

The function is explicitly made public to facilitate development of custom WAL reading applications, particularly background workers that need to read WAL data but operate outside the walsender infrastructure. Unlike the walsender's specialized version, this function uses a check/sleep/repeat loop for waiting on WAL availability since normal backends lack the latch-based notification infrastructure available to walsenders.

The function passes the 'wait' parameter as true to read_local_xlog_page_guts, meaning it will wait for WAL to become available rather than returning immediately if the requested data is not yet present.

## Parameters / Member Variables
- `state`: XLogReaderState containing reading context and segment information
- `targetPagePtr`: XLogRecPtr specifying the WAL page to read (must be page-aligned)
- `reqLen`: Number of bytes requested from the page (typically XLOG_BLCKSZ)
- `targetRecPtr`: XLogRecPtr of the specific record being targeted within the page
- `cur_page`: Buffer to store the read page data (must be at least reqLen bytes)

## Dependencies
- Functions called/Symbols referenced:
  - [read_local_xlog_page_guts](read_local_xlog_page_guts.md) (with wait=true parameter)
- Called from (representative examples):
  - [XlogReadTwoPhaseData](../X/XlogReadTwoPhaseData.md)
  - LogicalReplicationSlotHasPendingWal
  - [LogicalSlotAdvanceAndCheckSnapState](../L/LogicalSlotAdvanceAndCheckSnapState.md)
  - [pg_logical_slot_get_changes_guts](../p/pg_logical_slot_get_changes_guts.md)
  - [create_logical_replication_slot](../c/create_logical_replication_slot.md)

## Notes and Other Information
- Explicitly designed as a public API for use outside walsender contexts
- Uses blocking wait behavior (unlike read_local_xlog_page_no_wait)
- Relies on check/sleep/repeat loop for WAL availability rather than latch-based notifications
- Particularly useful for background workers that need WAL reading capabilities
- Return value follows XLogReaderRoutine page_read callback conventions
- The TODO comment indicates potential future optimization with better notification infrastructure
- Primarily used in logical replication and two-phase commit recovery scenarios