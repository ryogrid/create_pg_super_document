# RemoveNonParentXlogFiles

## Location
[src/backend/access/transam/xlog.c:3917-3985](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlog.c#L3917-L3985)

## Overview
Removes or recycles WAL files that are not part of the current timeline's history during recovery timeline switches, preventing garbage data from being archived.

## Definition


## Detailed Description
This function is called during WAL recovery when switching to a new timeline or at the end of recovery when creating a new timeline. It identifies and removes WAL segments that belong to timelines not in the ancestry of the current timeline to prevent archiving of potentially garbage data.

The function operates by:
1. Calculating the switch segment number from the switchpoint
2. Determining recycling boundaries (arbitrarily recycling 10 future segments)
3. Constructing the filename of the last segment to be preserved on the new timeline
4. Scanning the pg_wal directory for WAL files
5. Identifying files from older timelines with segment numbers >= the switch segment
6. Removing files that haven't been marked as ready for archiving

This cleanup is essential because leftover pre-allocated or recycled WAL segments from the old timeline might contain garbage data that shouldn't be archived. Files from our timeline history are safe because they've been successfully replayed.

## Parameters / Member Variables
- : The WAL position where the timeline switch occurs
- : The new timeline ID we're switching to

## Dependencies
- Functions called/Symbols referenced:
  - XLByteToPrevSeg
  - XLByteToSeg  
  - [XLogFileName](../X/XLogFileName.md)
  - AllocateDir
  - ReadDir
  - FreeDir
  - [IsXLogFileName](../I/IsXLogFileName.md)
  - [XLogArchiveIsReady](../X/XLogArchiveIsReady.md)
  - [RemoveXlogFile](RemoveXlogFile.md)
  - elog
- Constants used:
  - XLOGDIR
  - DEBUG2
  - MAXFNAMELEN
  - wal_segment_size
- Called from:
  - [CleanupAfterArchiveRecovery](../C/CleanupAfterArchiveRecovery.md)
  - [ApplyWalRecord](../A/ApplyWalRecord.md)

## Notes and Other Information
- This function is declared as public (not static), making it accessible from other files
- The function conservatively preserves files that are already marked as .ready for archiving, letting them be archived and removed later
- Recycling is set to handle 10 future segments somewhat arbitrarily
- Timeline comparison uses string comparison of the first 8 characters (timeline portion) and segment portion separately
- The cleanup prevents garbage WAL data from contaminating the archive
- File path: src/backend/access/transam/xlog.c:3917-3985