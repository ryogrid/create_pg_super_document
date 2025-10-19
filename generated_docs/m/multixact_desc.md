# multixact_desc

## Location
[src/backend/access/rmgrdesc/mxactdesc.c:50-83](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/rmgrdesc/mxactdesc.c#L50-L83)

## Overview
A WAL record description function that formats multixact-related transaction log records into human-readable text for debugging and analysis purposes.

## Definition
```c
void multixact_desc(StringInfo buf, XLogReaderState *record)
```

## Detailed Description
The `multixact_desc` function is part of PostgreSQL's WAL (Write-Ahead Logging) record description system. It parses and formats multixact-related log records into readable text output. The function handles three types of multixact operations:

1. **ZERO_OFF_PAGE/ZERO_MEM_PAGE**: Operations that zero out multixact offset or member pages, displaying the page number
2. **CREATE_ID**: MultiXact creation records, showing the multixact ID, offset, member count, and detailed information about each member transaction
3. **TRUNCATE_ID**: MultiXact truncation records, displaying the truncation ranges for both offsets and members

This function is essential for WAL analysis, debugging multixact issues, and understanding transaction behavior in PostgreSQL.

## Parameters / Member Variables
- `buf`: StringInfo buffer where the formatted description will be appended
- `record`: XLogReaderState pointer containing the WAL record data to be described

## Dependencies
- Functions called/Symbols referenced:
  - XLogRecGetData
  - XLogRecGetInfo
  - [appendStringInfo](../a/appendStringInfo.md)
  - [appendStringInfoString](../a/appendStringInfoString.md)
  - memcpy
  - [out_member](../o/out_member.md)
  - XLR_INFO_MASK
  - XLOG_MULTIXACT_ZERO_OFF_PAGE
  - XLOG_MULTIXACT_ZERO_MEM_PAGE
  - XLOG_MULTIXACT_CREATE_ID
  - XLOG_MULTIXACT_TRUNCATE_ID
  - [xl_multixact_create](../x/xl_multixact_create.md) (struct)
  - [xl_multixact_truncate](../x/xl_multixact_truncate.md) (struct)
- Called from (representative examples):
  - WAL record description system (referenced in SizeOfMultiXactTruncate)

## Notes and Other Information
- Part of the resource manager description interface for multixact operations
- Uses bit masking with XLR_INFO_MASK to extract the actual operation type from WAL record info
- Handles variable-length records appropriately for CREATE_ID operations with multiple members
- Critical for debugging multixact-related issues and understanding transaction concurrency behavior
- The function provides detailed member information by calling out_member for each transaction in CREATE_ID records

## Simplified Source

```c
void multixact_desc(StringInfo buf, XLogReaderState *record) {
    char *rec = XLogRecGetData(record);
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

    if (info == XLOG_MULTIXACT_ZERO_OFF_PAGE || info == XLOG_MULTIXACT_ZERO_MEM_PAGE) {
        // Handle page zeroing operations
        int64 pageno;
        memcpy(&pageno, rec, sizeof(pageno));
        appendStringInfo(buf, "%lld", (long long) pageno);
    }
    else if (info == XLOG_MULTIXACT_CREATE_ID) {
        // Handle multixact creation
        xl_multixact_create *xlrec = (xl_multixact_create *) rec;
        appendStringInfo(buf, "%u offset %u nmembers %d: ",
                         xlrec->mid, xlrec->moff, xlrec->nmembers);

        // Output each member transaction
        for (int i = 0; i < xlrec->nmembers; i++)
            out_member(buf, &xlrec->members[i]);
    }
    else if (info == XLOG_MULTIXACT_TRUNCATE_ID) {
        // Handle multixact truncation
        xl_multixact_truncate *xlrec = (xl_multixact_truncate *) rec;
        appendStringInfo(buf, "offsets [%u, %u), members [%u, %u)",
                         xlrec->startTruncOff, xlrec->endTruncOff,
                         xlrec->startTruncMemb, xlrec->endTruncMemb);
    }
}
```