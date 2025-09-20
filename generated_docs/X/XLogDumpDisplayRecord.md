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
- : XLogDumpConfig containing configuration options that control output formatting and detail level
- : XLogReaderState containing the decoded WAL record to display

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
  - initStringInfo
  - resetStringInfo
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