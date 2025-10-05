# xlog_desc

## Location
[src/backend/access/rmgrdesc/xlogdesc.c:58-172](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/rmgrdesc/xlogdesc.c#L58-L172)

## Overview
Generates human-readable descriptions of XLOG (transaction log) records for debugging and diagnostic purposes.

## Definition

```c
void
xlog_desc(StringInfo buf, XLogReaderState *record)
```
## Detailed Description
This function is a resource manager descriptor function specifically for XLOG records. It parses different types of WAL (Write-Ahead Log) records and formats them into human-readable descriptions that are appended to a StringInfo buffer. The function handles various XLOG record types including checkpoints, parameter changes, restore points, full-page writes, backup operations, and recovery-related records.

The function uses a switch-like structure based on the record's info field to determine the record type and format the appropriate description. Each record type has its own specific formatting logic to display relevant information such as LSN positions, transaction IDs, configuration parameters, and timestamps.

## Parameters / Member Variables
- `buf`: StringInfo buffer where the formatted description will be appended
- `record`: XLogReaderState containing the WAL record to be described

## Dependencies
- Functions called/Symbols referenced:
  - XLogRecGetData (extracts record data)
  - XLogRecGetInfo (extracts record info flags)  
  - [get_wal_level_string](../g/get_wal_level_string.md) (converts WAL level to string)
  - EpochFromFullTransactionId, XidFromFullTransactionId (transaction ID utilities)
  - [timestamptz_to_str](../t/timestamptz_to_str.md) (timestamp formatting)
  - [appendStringInfo](../a/appendStringInfo.md), appendStringInfoString (string buffer operations)
- Called from (representative examples):
  - Resource manager framework via rmgrlist.h registration
  - WAL record debugging and analysis tools

## Notes and Other Information
- This function is registered in the resource manager list (rmgrlist.h) as the descriptor function for XLOG records
- Handles multiple XLOG record types: CHECKPOINT_SHUTDOWN/ONLINE, NEXTOID, RESTORE_POINT, FPI, BACKUP_END, PARAMETER_CHANGE, FPW_CHANGE, END_OF_RECOVERY, OVERWRITE_CONTRECORD, CHECKPOINT_REDO
- Each record type has specific formatting to show the most relevant information for that operation
- Used primarily for debugging, logging, and WAL analysis tools like pg_waldump
- The function doesn't modify the input record, only reads from it to generate descriptions

## Simplified Source

```c
void xlog_desc(StringInfo buf, XLogReaderState *record) {
    char *rec = XLogRecGetData(record);
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

    // Handle checkpoint records (shutdown and online)
    if (info == XLOG_CHECKPOINT_SHUTDOWN || info == XLOG_CHECKPOINT_ONLINE) {
        CheckPoint *checkpoint = (CheckPoint *) rec;

        appendStringInfo(buf, "redo %X/%X; "
                        "tli %u; prev tli %u; fpw %s; wal_level %s; xid %u:%u; oid %u; multi %u; offset %u; "
                        "oldest xid %u in DB %u; oldest multi %u in DB %u; "
                        "oldest/newest commit timestamp xid: %u/%u; "
                        "oldest running xid %u; %s",
                        LSN_FORMAT_ARGS(checkpoint->redo),
                        checkpoint->ThisTimeLineID,
                        checkpoint->PrevTimeLineID,
                        checkpoint->fullPageWrites ? "true" : "false",
                        get_wal_level_string(checkpoint->wal_level),
                        EpochFromFullTransactionId(checkpoint->nextXid),
                        XidFromFullTransactionId(checkpoint->nextXid),
                        checkpoint->nextOid,
                        checkpoint->nextMulti,
                        checkpoint->nextMultiOffset,
                        checkpoint->oldestXid,
                        checkpoint->oldestXidDB,
                        checkpoint->oldestMulti,
                        checkpoint->oldestMultiDB,
                        checkpoint->oldestCommitTsXid,
                        checkpoint->newestCommitTsXid,
                        checkpoint->oldestActiveXid,
                        (info == XLOG_CHECKPOINT_SHUTDOWN) ? "shutdown" : "online");
    }
    // Handle next OID assignment
    else if (info == XLOG_NEXTOID) {
        Oid nextOid;
        memcpy(&nextOid, rec, sizeof(Oid));
        appendStringInfo(buf, "%u", nextOid);
    }
    // Handle restore points
    else if (info == XLOG_RESTORE_POINT) {
        xl_restore_point *xlrec = (xl_restore_point *) rec;
        appendStringInfoString(buf, xlrec->rp_name);
    }
    // Handle full page images
    else if (info == XLOG_FPI || info == XLOG_FPI_FOR_HINT) {
        // No additional information to print
    }
    // Handle backup end records
    else if (info == XLOG_BACKUP_END) {
        XLogRecPtr startpoint;
        memcpy(&startpoint, rec, sizeof(XLogRecPtr));
        appendStringInfo(buf, "%X/%X", LSN_FORMAT_ARGS(startpoint));
    }
    // Handle parameter changes
    else if (info == XLOG_PARAMETER_CHANGE) {
        xl_parameter_change xlrec;
        memcpy(&xlrec, rec, sizeof(xl_parameter_change));

        appendStringInfo(buf, "max_connections=%d max_worker_processes=%d "
                        "max_wal_senders=%d max_prepared_xacts=%d "
                        "max_locks_per_xact=%d wal_level=%s "
                        "wal_log_hints=%s track_commit_timestamp=%s",
                        xlrec.MaxConnections,
                        xlrec.max_worker_processes,
                        xlrec.max_wal_senders,
                        xlrec.max_prepared_xacts,
                        xlrec.max_locks_per_xact,
                        get_wal_level_string(xlrec.wal_level),
                        xlrec.wal_log_hints ? "on" : "off",
                        xlrec.track_commit_timestamp ? "on" : "off");
    }
    // Handle full page write changes
    else if (info == XLOG_FPW_CHANGE) {
        bool fpw;
        memcpy(&fpw, rec, sizeof(bool));
        appendStringInfoString(buf, fpw ? "true" : "false");
    }
    // Handle end of recovery
    else if (info == XLOG_END_OF_RECOVERY) {
        xl_end_of_recovery xlrec;
        memcpy(&xlrec, rec, sizeof(xl_end_of_recovery));
        appendStringInfo(buf, "tli %u; prev tli %u; time %s; wal_level %s",
                        xlrec.ThisTimeLineID, xlrec.PrevTimeLineID,
                        timestamptz_to_str(xlrec.end_time),
                        get_wal_level_string(xlrec.wal_level));
    }
    // Handle overwrite continuation records
    else if (info == XLOG_OVERWRITE_CONTRECORD) {
        xl_overwrite_contrecord xlrec;
        memcpy(&xlrec, rec, sizeof(xl_overwrite_contrecord));
        appendStringInfo(buf, "lsn %X/%X; time %s",
                        LSN_FORMAT_ARGS(xlrec.overwritten_lsn),
                        timestamptz_to_str(xlrec.overwrite_time));
    }
    // Handle checkpoint redo records
    else if (info == XLOG_CHECKPOINT_REDO) {
        int wal_level;
        memcpy(&wal_level, rec, sizeof(int));
        appendStringInfo(buf, "wal_level %s", get_wal_level_string(wal_level));
    }
}
```