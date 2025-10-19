# findLastCheckpoint

## Location
[src/bin/pg_rewind/parsexlog.c:168-274](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_rewind/parsexlog.c#L168-L274)

## Overview
Searches backwards through WAL records from a given fork point to find the last checkpoint record that occurred before the fork, essential for pg_rewind to determine the safe starting point for synchronization.

## Definition

```c
void
findLastCheckpoint(const char *datadir, XLogRecPtr forkptr, int tliIndex,
				   XLogRecPtr *lastchkptrec, TimeLineID *lastchkpttli,
				   XLogRecPtr *lastchkptredo, const char *restoreCommand)
```
## Detailed Description
This function implements a critical part of pg_rewind's WAL analysis by walking backwards through WAL records to locate the most recent checkpoint that occurred before the WAL fork point. The checkpoint found serves as the safe starting point for data synchronization between the source and target PostgreSQL instances.

The function handles WAL page boundaries correctly, skipping page headers when the fork pointer falls exactly at a page boundary. It reads WAL records backwards by following the xl_prev chain and identifies checkpoint records by examining their resource manager ID and record type. When a valid checkpoint is found (either XLOG_CHECKPOINT_SHUTDOWN or XLOG_CHECKPOINT_ONLINE), it extracts the checkpoint data and returns the relevant information through output parameters.

Additionally, the function tracks WAL filenames that should be preserved during the rewind process by calling keepwal_add_entry() for each WAL segment encountered.

## Parameters / Member Variables
- `*datadir`: Path to the PostgreSQL data directory containing pg_wal subdirectory
- `forkptr`: XLogRecPtr indicating the WAL position where the fork occurred
- `tliIndex`: Index into the target timeline history array indicating which timeline to search
- `*lastchkptrec`: Output parameter - XLogRecPtr of the found checkpoint record
- `*lastchkpttli`: Output parameter - TimeLineID of the found checkpoint
- `*lastchkptredo`: Output parameter - XLogRecPtr of the checkpoint's redo point
- `*restoreCommand`: Command string used to restore archived WAL files if needed (can be NULL)
## Dependencies
- Functions called/Symbols referenced:
  - [XLogReaderAllocate](../X/XLogReaderAllocate.md)
  - [SimpleXLogPageRead](../S/SimpleXLogPageRead.md)
  - [XLogBeginRead](../X/XLogBeginRead.md)
  - [XLogReadRecord](../X/XLogReadRecord.md)
  - XLogSegmentOffset
  - [XLogFileName](../X/XLogFileName.md)
  - XLogRecGetInfo
  - XLogRecGetRmid
  - XLogRecGetData
  - [keepwal_add_entry](../k/keepwal_add_entry.md)
  - [XLogReaderFree](../X/XLogReaderFree.md)
  - [XLogRecord](../X/XLogRecord.md)
  - [CheckPoint](../C/CheckPoint.md)
- Called from (representative examples):
  - [main](../m/main.md) (in src/bin/pg_rewind/pg_rewind.c:461)

## Notes and Other Information
- The search stops at the first checkpoint found before the fork point, not the most recent overall checkpoint
- Properly handles page boundary conditions where fork pointer aligns with page headers
- Tracks WAL files that must be preserved during the rewind operation
- Uses backward chaining through xl_prev pointers to traverse WAL history efficiently
- Critical for ensuring pg_rewind starts from a consistent checkpoint state
- The checkpoint found determines the scope of data that needs to be synchronized

## Simplified Source

```c
void findLastCheckpoint(const char *datadir, XLogRecPtr forkptr, int tliIndex,
                       XLogRecPtr *lastchkptrec, TimeLineID *lastchkpttli,
                       XLogRecPtr *lastchkptredo, const char *restoreCommand)
{
    XLogRecord *record;
    XLogRecPtr searchptr;
    XLogReaderState *xlogreader;
    XLogPageReadPrivate private;

    // Skip page header if fork pointer is at page boundary
    if (forkptr % XLOG_BLCKSZ == 0) {
        if (XLogSegmentOffset(forkptr, WalSegSz) == 0)
            forkptr += SizeOfXLogLongPHD;
        else
            forkptr += SizeOfXLogShortPHD;
    }

    // Initialize WAL reader for backward traversal
    private.tliIndex = tliIndex;
    private.restoreCommand = restoreCommand;
    xlogreader = XLogReaderAllocate(WalSegSz, datadir,
                                   XL_ROUTINE(.page_read = &SimpleXLogPageRead),
                                   &private);
    if (xlogreader == NULL)
        pg_fatal("out of memory while allocating a WAL reading processor");

    // Walk backwards through WAL records to find checkpoint
    searchptr = forkptr;
    for (;;) {
        XLogBeginRead(xlogreader, searchptr);
        record = XLogReadRecord(xlogreader, &errormsg);

        if (record == NULL) {
            pg_fatal("could not find previous WAL record at %X/%X",
                    LSN_FORMAT_ARGS(searchptr));
        }

        // Track WAL files to preserve during rewind
        keepwal_add_entry(current_xlog_filename);

        // Check if this is a checkpoint record before the fork point
        uint8 info = XLogRecGetInfo(xlogreader) & ~XLR_INFO_MASK;
        if (searchptr < forkptr &&
            XLogRecGetRmid(xlogreader) == RM_XLOG_ID &&
            (info == XLOG_CHECKPOINT_SHUTDOWN || info == XLOG_CHECKPOINT_ONLINE)) {

            // Found checkpoint - extract checkpoint data
            CheckPoint checkPoint;
            memcpy(&checkPoint, XLogRecGetData(xlogreader), sizeof(CheckPoint));
            *lastchkptrec = searchptr;
            *lastchkpttli = checkPoint.ThisTimeLineID;
            *lastchkptredo = checkPoint.redo;
            break;
        }

        // Continue backwards to previous record
        searchptr = record->xl_prev;
    }

    // Cleanup
    XLogReaderFree(xlogreader);
}
```