# logical_read_xlog_page

## Location
src/backend/replication/walsender.c: 1055 - 1126

## Overview
A WAL page reading callback function optimized for logical decoding contexts in walsender processes, providing efficient WAL data access by leveraging the walsender's latch mechanism instead of busy-waiting.

## Definition


## Detailed Description
logical_read_xlog_page serves as an XLogReaderRoutine page_read callback specifically designed for logical decoding in walsender processes. This function provides several optimizations over the standard read_local_xlog_page:

1. **Efficient Waiting**: Uses WalSndWaitForWal which leverages the walsender's latch that gets set when WAL is flushed, avoiding busy-loop waiting
2. **Timeline Management**: Properly handles timeline changes and determines the current timeline for both primary and standby servers
3. **Recovery Awareness**: Adapts behavior based on whether the server is in recovery mode
4. **Data Validation**: Ensures the read WAL data is still valid after reading, protecting against concurrent WAL recycling

The function handles both standalone and cascading replication scenarios, automatically determining the appropriate timeline and ensuring that logical decoding works correctly during timeline switches and server promotions.

## Parameters / Member Variables
- : XLogReaderState containing the current state of WAL reading operations
- : WAL pointer of the page to be read
- : Number of bytes requested to be read from the target page
- : WAL pointer of the specific record being targeted (used for validation)
- : Buffer where the read WAL page data will be stored

## Dependencies
- Functions called/Symbols referenced:
  - [WalSndWaitForWal](../W/WalSndWaitForWal.md)
  - [RecoveryInProgress](../R/RecoveryInProgress.md)
  - [GetXLogReplayRecPtr](../G/GetXLogReplayRecPtr.md)
  - [GetWALInsertionTimeLine](../G/GetWALInsertionTimeLine.md)
  - [XLogReadDetermineTimeline](../X/XLogReadDetermineTimeline.md)
  - [WALRead](../W/WALRead.md)
  - [WALReadRaiseError](../W/WALReadRaiseError.md)
  - XLByteToSeg
  - [CheckXLogRemoved](../C/CheckXLogRemoved.md)
- Called from:
  - [CreateReplicationSlot](../C/CreateReplicationSlot.md)
  - [StartLogicalReplication](../S/StartLogicalReplication.md)

## Notes and Other Information
- Optimized specifically for walsender processes performing logical decoding
- Automatically handles timeline transitions and server promotions
- Returns -1 if insufficient WAL is available, indicating shutdown condition
- Returns the actual number of bytes read (either XLOG_BLCKSZ for full pages or partial count)
- Updates global timeline tracking variables (sendTimeLineIsHistoric, sendTimeLine, etc.)
- Validates WAL segment availability after reading to detect concurrent WAL recycling
- Used by logical replication infrastructure to efficiently stream decoded changes
- The function assumes that WAL segments are managed by WalSndSegmentOpen for timeline transitions