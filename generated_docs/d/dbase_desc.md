# dbase_desc

## Location
[src/backend/access/rmgrdesc/dbasedesc.c:22-56](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/rmgrdesc/dbasedesc.c#L22-L56)

## Overview
Generates human-readable descriptions of database-related WAL (Write-Ahead Log) records for debugging and monitoring purposes.

## Definition

```c
void
dbase_desc(StringInfo buf, XLogReaderState *record)
```
## Detailed Description
The  function is part of PostgreSQL's WAL record description system, specifically for database management operations. It parses database-related WAL records and appends human-readable descriptions to a string buffer. This function is primarily used by tools like  to provide meaningful output when examining WAL files. The function handles three types of database operations: creating databases by copying files, creating databases using WAL logging, and dropping databases.

## Parameters / Member Variables
- `buf`: StringInfo buffer where the human-readable description will be appended
- `*record`: XLogReaderState pointer containing the WAL record data to be described
## Dependencies
- Functions called/Symbols referenced:
  - XLogRecGetData
  - XLogRecGetInfo
  - [appendStringInfo](../a/appendStringInfo.md)
  - [appendStringInfoString](../a/appendStringInfoString.md)
- Constants used:
  - XLR_INFO_MASK
  - XLOG_DBASE_CREATE_FILE_COPY
  - XLOG_DBASE_CREATE_WAL_LOG
  - XLOG_DBASE_DROP
- Structures used:
  - [xl_dbase_create_file_copy_rec](../x/xl_dbase_create_file_copy_rec.md)
  - [xl_dbase_create_wal_log_rec](../x/xl_dbase_create_wal_log_rec.md)
  - [xl_dbase_drop_rec](../x/xl_dbase_drop_rec.md)
- Called from (representative examples):
  - WAL dump utilities and debugging tools

## Notes and Other Information
- This function is located in src/backend/access/rmgrdesc/dbasedesc.c:22-56
- It handles three specific database operation types identified by info flags
- For CREATE_FILE_COPY operations, it shows source and destination tablespace/database IDs
- For CREATE_WAL_LOG operations, it shows the created tablespace and database IDs
- For DROP operations, it lists all tablespaces containing the dropped database
- The function is part of the resource manager description system for database operations

## Simplified Source

```c
void
dbase_desc(StringInfo buf, XLogReaderState *record)
{
    char       *rec = XLogRecGetData(record);
    uint8       info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

    if (info == XLOG_DBASE_CREATE_FILE_COPY)
    {
        // Describe database creation by copying files
        xl_dbase_create_file_copy_rec *xlrec = (xl_dbase_create_file_copy_rec *) rec;
        appendStringInfo(buf, "copy dir %u/%u to %u/%u",
                         xlrec->src_tablespace_id, xlrec->src_db_id,
                         xlrec->tablespace_id, xlrec->db_id);
    }
    else if (info == XLOG_DBASE_CREATE_WAL_LOG)
    {
        // Describe database creation using WAL logging
        xl_dbase_create_wal_log_rec *xlrec = (xl_dbase_create_wal_log_rec *) rec;
        appendStringInfo(buf, "create dir %u/%u",
                         xlrec->tablespace_id, xlrec->db_id);
    }
    else if (info == XLOG_DBASE_DROP)
    {
        // Describe database drop operation
        xl_dbase_drop_rec *xlrec = (xl_dbase_drop_rec *) rec;

        appendStringInfoString(buf, "dir");
        for (int i = 0; i < xlrec->ntablespaces; i++)
            appendStringInfo(buf, " %u/%u",
                             xlrec->tablespace_ids[i], xlrec->db_id);
    }
}
```