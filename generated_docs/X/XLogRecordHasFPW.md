# XLogRecordHasFPW

## Location
src/bin/pg_waldump/pg_waldump.c: 469 - 489

## Overview
XLogRecordHasFPW determines whether a given WAL record contains any full page writes (FPW), which are complete page images stored in WAL records.

## Definition


## Detailed Description
This function iterates through all block references in a WAL record to check if any of them contain a full page write. Full page writes occur when PostgreSQL needs to store a complete copy of a data page in the WAL, typically after a checkpoint or when the page is first modified after being read from disk. The function is used by pg_waldump to filter records that contain FPWs, which is useful for analyzing WAL overhead and understanding when complete page images are being written.

## Parameters / Member Variables
- : XLogReaderState containing the decoded WAL record to examine for full page writes

## Dependencies
- Functions called/Symbols referenced:
  - XLogRecMaxBlockId
  - XLogRecHasBlockRef
  - XLogRecHasBlockImage
- Called from (representative examples):
  - [main](../m/main.md) (used for filtering records with full page writes in pg_waldump)

## Notes and Other Information
- Returns true if at least one block in the record contains a full page write
- Full page writes significantly increase WAL volume but provide crash recovery safety
- Used to implement the --fpw option in pg_waldump for analyzing full page write behavior
- Checks all valid block references in the record (from 0 to XLogRecMaxBlockId)
- Essential for understanding WAL overhead and optimizing checkpoint frequency