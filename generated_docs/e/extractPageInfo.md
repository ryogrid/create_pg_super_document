# extractPageInfo

## Location
[src/bin/pg_rewind/parsexlog.c:389-483](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_rewind/parsexlog.c#L389-L483)

## Overview
Analyzes individual WAL records to identify which data blocks are modified and adds them to the page map used by pg_rewind for determining file synchronization requirements.

## Definition

```c
static void
extractPageInfo(XLogReaderState *record)
```
## Detailed Description
This function is the core page analysis component of pg_rewind's WAL processing system. It examines individual WAL records to determine which data blocks have been modified and need to be considered during the rewind operation. The function implements specific handling logic for various types of WAL records, categorizing them based on their impact on data files.

The function recognizes several categories of WAL records: database creation/deletion operations (which can be safely ignored as entire databases are handled separately), storage manager operations like file creation and truncation (also safely ignored), and transaction commit/abort records (which may contain dropped relation information but don't require special page tracking).

For records that modify actual data blocks, the function iterates through all blocks referenced by the WAL record, extracting the RelFileLocator, fork number, and block number. It focuses only on the main fork of relations, as other forks (visibility map, free space map) are copied in their entirety. For each main fork block, it calls process_target_wal_block_change() to register the block in the page map.

## Parameters / Member Variables
- `*record`: XLogReaderState containing the current WAL record to analyze
## Dependencies
- Functions called/Symbols referenced:
  - XLogRecGetRmid
  - XLogRecGetInfo
  - XLogRecMaxBlockId
  - [XLogRecGetBlockTagExtended](../X/XLogRecGetBlockTagExtended.md)
  - RmgrName
  - [process_target_wal_block_change](../p/process_target_wal_block_change.md)
  - RmgrId
  - [RelFileLocator](../R/RelFileLocator.md)
  - [ForkNumber](../F/ForkNumber.md)
  - BlockNumber
- Called from (representative examples):
  - [extractPageMap](extractPageMap.md) (in src/bin/pg_rewind/parsexlog.c:100)

## Notes and Other Information
- Implements selective processing based on WAL record type (rmid and rminfo)
- Only tracks changes to MAIN_FORKNUM - other forks are copied completely
- Includes safety logic for database and storage manager operations that don't require block-level tracking  
- Contains fatal error handling for unrecognized special relation update records
- Critical for building the accurate page map that drives pg_rewind's selective file copying
- The function's logic determines which blocks need to be copied vs. which can be safely ignored
- Works in conjunction with process_target_wal_block_change() to maintain the global page map

## Simplified Source

```c
static void extractPageInfo(XLogReaderState *record)
{
    RmgrId rmid = XLogRecGetRmid(record);
    uint8 info = XLogRecGetInfo(record);
    uint8 rminfo = info & ~XLR_INFO_MASK;

    // Handle special record types that can be safely ignored
    if (rmid == RM_DBASE_ID &&
        (rminfo == XLOG_DBASE_CREATE_FILE_COPY ||
         rminfo == XLOG_DBASE_CREATE_WAL_LOG ||
         rminfo == XLOG_DBASE_DROP)) {
        // Database operations - entire databases are handled separately
        return;
    }

    if (rmid == RM_SMGR_ID &&
        (rminfo == XLOG_SMGR_CREATE || rminfo == XLOG_SMGR_TRUNCATE)) {
        // Storage manager operations - handled by file comparison
        return;
    }

    if (rmid == RM_XACT_ID &&
        ((rminfo & XLOG_XACT_OPMASK) == XLOG_XACT_COMMIT ||
         (rminfo & XLOG_XACT_OPMASK) == XLOG_XACT_COMMIT_PREPARED ||
         (rminfo & XLOG_XACT_OPMASK) == XLOG_XACT_ABORT ||
         (rminfo & XLOG_XACT_OPMASK) == XLOG_XACT_ABORT_PREPARED)) {
        // Transaction records with dropped rels - handled by file comparison
        return;
    }

    // Check for unrecognized special relation updates
    if (info & XLR_SPECIAL_REL_UPDATE) {
        pg_fatal("WAL record modifies a relation, but record type is not recognized: "
                "lsn: %X/%X, rmid: %d, rmgr: %s, info: %02X",
                LSN_FORMAT_ARGS(record->ReadRecPtr),
                rmid, RmgrName(rmid), info);
    }

    // Process all data blocks modified by this record
    for (int block_id = 0; block_id <= XLogRecMaxBlockId(record); block_id++) {
        RelFileLocator rlocator;
        ForkNumber forknum;
        BlockNumber blkno;

        // Extract block information from WAL record
        if (!XLogRecGetBlockTagExtended(record, block_id, &rlocator, &forknum, &blkno, NULL))
            continue;

        // Only track main fork changes - other forks copied completely
        if (forknum != MAIN_FORKNUM)
            continue;

        // Add this block to the page map
        process_target_wal_block_change(forknum, rlocator, blkno);
    }
}
```