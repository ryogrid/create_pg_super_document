# XLogRecordHasFPW

## Location
[src/bin/pg_waldump/pg_waldump.c:469-489](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_waldump/pg_waldump.c#L469-L489)

## Overview
XLogRecordHasFPW determines whether a given WAL record contains any full page writes (FPW), which are complete page images stored in WAL records.

## Definition

```c
static bool
XLogRecordHasFPW(XLogReaderState *record)
```
## Detailed Description
This function iterates through all block references in a WAL record to check if any of them contain a full page write. Full page writes occur when PostgreSQL needs to store a complete copy of a data page in the WAL, typically after a checkpoint or when the page is first modified after being read from disk. The function is used by pg_waldump to filter records that contain FPWs, which is useful for analyzing WAL overhead and understanding when complete page images are being written.

## Parameters / Member Variables
- `*record`: XLogReaderState containing the decoded WAL record to examine for full page writes
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

## Simplified Source

```c
static bool
XLogRecordHasFPW(XLogReaderState *record)
{
    // Check each block reference in the record
    for (int block_id = 0; block_id <= XLogRecMaxBlockId(record); block_id++) {
        if (!XLogRecHasBlockRef(record, block_id))
            continue;

        // Return true if any block has a full page image
        if (XLogRecHasBlockImage(record, block_id))
            return true;
    }

    return false;
}
```