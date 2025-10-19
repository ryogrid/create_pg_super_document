# XLogDumpDisplayRecord

## Location
[src/bin/pg_waldump/pg_waldump.c:546-584](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_waldump/pg_waldump.c#L546-L584)

## Overview
XLogDumpDisplayRecord formats and prints a comprehensive, human-readable representation of a WAL record to stdout.

## Definition

```c
static void
XLogDumpDisplayRecord(XLogDumpConfig *config, XLogReaderState *record)
```
## Detailed Description
This function produces the main output format for pg_waldump, displaying detailed information about WAL records in a structured, human-readable format. It extracts and displays key metadata including resource manager information, record lengths, transaction IDs, LSR values, and previous record pointers. The function leverages the PostgreSQL resource manager infrastructure to provide operation-specific descriptions and detailed block reference information. The output format is designed to be both human-readable for manual analysis and parseable for automated processing.

## Parameters / Member Variables
- `*config`: XLogDumpConfig containing configuration options that control output formatting and detail level
- `*record`: XLogReaderState containing the decoded WAL record to display
## Dependencies
- Functions called/Symbols referenced:
  - [GetRmgrDesc](../G/GetRmgrDesc.md)
  - XLogRecGetRmid
  - XLogRecGetInfo
  - XLogRecGetPrev
  - [XLogRecGetLen](XLogRecGetLen.md)
  - XLogRecGetTotalLen
  - XLogRecGetXid
  - [XLogRecGetBlockRefInfo](XLogRecGetBlockRefInfo.md)
  - [initStringInfo](../i/initStringInfo.md)
  - [resetStringInfo](../r/resetStringInfo.md)
  - [pfree](../p/pfree.md)
  - [RmgrDescData](../R/RmgrDescData.md) (type)
  - [XLogDumpConfig](XLogDumpConfig.md) (type)
  - XLR_INFO_MASK (constant)
- Called from (representative examples):
  - [main](../m/main.md) (primary output function in pg_waldump)

## Notes and Other Information
- Produces the standard pg_waldump output format with rmgr, length, tx, lsn, and description fields
- Uses resource manager callbacks (rm_identify, rm_desc) for operation-specific formatting
- Handles unknown or unregistered operation types gracefully
- Includes optional detailed block reference information based on configuration
- Output format: 'rmgr: NAME len (rec/tot): X/Y, tx: Z, lsn: A/B, prev C/D, desc: OPERATION details'
- Essential function for WAL analysis and debugging in PostgreSQL

## Simplified Source

```c
static void
XLogDumpDisplayRecord(XLogDumpConfig *config, XLogReaderState *record)
{
    const char *id;
    const RmgrDescData *desc = GetRmgrDesc(XLogRecGetRmid(record));
    uint32 rec_len;
    uint32 fpi_len;
    uint8 info = XLogRecGetInfo(record);
    XLogRecPtr xl_prev = XLogRecGetPrev(record);
    StringInfoData s;

    // Get record length information
    XLogRecGetLen(record, &rec_len, &fpi_len);

    // Print basic record information: manager, lengths, transaction, LSN
    printf("rmgr: %-11s len (rec/tot): %6u/%6u, tx: %10u, lsn: %X/%08X, prev %X/%08X, ",
           desc->rm_name,
           rec_len, XLogRecGetTotalLen(record),
           XLogRecGetXid(record),
           LSN_FORMAT_ARGS(record->ReadRecPtr),
           LSN_FORMAT_ARGS(xl_prev));

    // Get operation description from resource manager
    id = desc->rm_identify(info);
    if (id == NULL)
        printf("desc: UNKNOWN (%x) ", info & ~XLR_INFO_MASK);
    else
        printf("desc: %s ", id);

    // Get detailed description from resource manager
    initStringInfo(&s);
    desc->rm_desc(&s, record);
    printf("%s", s.data);

    // Add block reference information if available
    resetStringInfo(&s);
    XLogRecGetBlockRefInfo(record, true, config->bkp_details, &s, NULL);
    printf("%s", s.data);
    pfree(s.data);
}
```