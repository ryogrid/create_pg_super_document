# commit_ts_desc

## Location
[src/backend/access/rmgrdesc/committsdesc.c:21-42](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/rmgrdesc/committsdesc.c#L21-L42)

## Overview
Provides human-readable descriptions of commit timestamp WAL record operations for debugging and logging purposes.

## Definition

```c
void
commit_ts_desc(StringInfo buf, XLogReaderState *record)
```
## Detailed Description
This function is part of PostgreSQL's WAL (Write-Ahead Logging) system and specifically handles the description of commit timestamp-related WAL records. It extracts information from WAL records and formats them into human-readable strings for debugging, logging, and diagnostic purposes. The function processes two types of commit timestamp operations: ZEROPAGE (for initializing pages) and TRUNCATE (for removing old commit timestamp data).

The function examines the WAL record's info field to determine the operation type and then formats the appropriate information into the provided string buffer. For ZEROPAGE operations, it displays the page number being zeroed. For TRUNCATE operations, it shows both the page number and the oldest transaction ID being preserved.

## Parameters / Member Variables
- `buf`: StringInfo buffer where the formatted description will be appended
- `record`: XLogReaderState containing the WAL record data to be described

## Dependencies
- Functions called/Symbols referenced:
  - XLogRecGetData
  - XLogRecGetInfo
  - XLR_INFO_MASK
  - COMMIT_TS_ZEROPAGE
  - COMMIT_TS_TRUNCATE
  - [xl_commit_ts_truncate](../x/xl_commit_ts_truncate.md)
  - [appendStringInfo](../a/appendStringInfo.md)
  - memcpy
- Called from (representative examples):
  - WAL record description framework (indirectly through function pointers)

## Notes and Other Information
- This function is part of the rmgrdesc (Resource Manager Description) system that provides human-readable descriptions of WAL records
- The function handles two specific commit timestamp WAL record types: ZEROPAGE (0x00) and TRUNCATE (0x10)
- Used primarily for debugging and diagnostic purposes when examining WAL files
- The output format includes specific details relevant to each operation type (page numbers, transaction IDs)
- Part of the commit timestamp tracking subsystem that was introduced to support logical replication features

## Simplified Source

```c
void
commit_ts_desc(StringInfo buf, XLogReaderState *record)
{
    char       *rec = XLogRecGetData(record);
    uint8       info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

    if (info == COMMIT_TS_ZEROPAGE)
    {
        // Describe commit timestamp page zeroing operation
        int64 pageno;
        memcpy(&pageno, rec, sizeof(pageno));
        appendStringInfo(buf, "%lld", (long long) pageno);
    }
    else if (info == COMMIT_TS_TRUNCATE)
    {
        // Describe commit timestamp truncate operation
        xl_commit_ts_truncate *trunc = (xl_commit_ts_truncate *) rec;
        appendStringInfo(buf, "pageno %lld, oldestXid %u",
                         (long long) trunc->pageno, trunc->oldestXid);
    }
}
```