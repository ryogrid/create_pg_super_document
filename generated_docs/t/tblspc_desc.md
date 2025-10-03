# tblspc_desc

## Location
[src/backend/access/rmgrdesc/tblspcdesc.c:21-40](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/rmgrdesc/tblspcdesc.c#L21-L40)

## Overview
Provides human-readable descriptions of tablespace-related WAL (Write-Ahead Logging) records for debugging and monitoring purposes.

## Definition

```c
void
tblspc_desc(StringInfo buf, XLogReaderState *record)
```
## Detailed Description
This function is part of PostgreSQL's WAL record description infrastructure, specifically designed to decode and format tablespace-related WAL records into readable text. It examines the WAL record type and extracts relevant information from CREATE and DROP tablespace operations, formatting this data into a string buffer for display in tools like pg_waldump.

The function handles two types of tablespace WAL records:
- **XLOG_TBLSPC_CREATE**: Formats the tablespace ID and path when a tablespace is created
- **XLOG_TBLSPC_DROP**: Formats only the tablespace ID when a tablespace is dropped

## Parameters / Member Variables
- `buf`: StringInfo buffer where the formatted description will be appended
- `*record`: XLogReaderState containing the WAL record data to be described
## Dependencies
- Functions called/Symbols referenced:
  - XLogRecGetData
  - XLogRecGetInfo
  - [appendStringInfo](../a/appendStringInfo.md)
- Constants referenced:
  - XLR_INFO_MASK
  - XLOG_TBLSPC_CREATE
  - XLOG_TBLSPC_DROP
- Structures used:
  - [xl_tblspc_create_rec](../x/xl_tblspc_create_rec.md)
  - [xl_tblspc_drop_rec](../x/xl_tblspc_drop_rec.md)
- Called from (representative examples):
  - WAL description infrastructure (no direct callers found in codebase)

## Notes and Other Information
- This function is part of the resource manager description interface for tablespaces
- Used primarily by debugging tools like pg_waldump to provide human-readable WAL record descriptions
- The function only handles the core tablespace operations (CREATE/DROP) and ignores other record types
- Output format for CREATE operations includes both tablespace ID and path in quotes
- Output format for DROP operations includes only the tablespace ID