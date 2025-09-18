# replorigin_desc

## Location
src/backend/access/rmgrdesc/replorigindesc.c: 19 - 50

## Overview
Generates human-readable descriptions of replication origin WAL records for debugging and logging purposes.

## Definition


## Detailed Description
The  function is part of PostgreSQL's WAL record description infrastructure, specifically handling replication origin-related WAL records. This function takes a WAL record and generates a human-readable string description of its contents, which is primarily used for debugging, logging, and WAL inspection tools like .

The function examines the info byte of the WAL record to determine the type of replication origin operation and formats the appropriate description:

- For  records: Shows the node ID, remote LSN, and force flag
- For  records: Shows the node ID being dropped

The output is appended to the provided StringInfo buffer, allowing for efficient string building without multiple memory allocations.

## Parameters / Member Variables
- : StringInfo buffer where the formatted description will be appended
- : XLogReaderState containing the WAL record to be described

## Dependencies
- Functions called/Symbols referenced:
  - XLogRecGetData
  - XLogRecGetInfo
  - appendStringInfo
  - LSN_FORMAT_ARGS
- Constants used:
  - XLR_INFO_MASK
  - XLOG_REPLORIGIN_SET
  - XLOG_REPLORIGIN_DROP
- Structures referenced:
  - [xl_replorigin_set](../x/xl_replorigin_set.md)
  - [xl_replorigin_drop](../x/xl_replorigin_drop.md)
- Called from (representative examples):
  - WAL description infrastructure
  - pg_waldump utility

## Notes and Other Information
- This function is part of the rmgr (resource manager) description system for WAL records
- The function handles only the defined replication origin record types; unknown types are silently ignored
- The LSN formatting uses the standard PostgreSQL LSN_FORMAT_ARGS macro for consistent output
- Located in src/backend/access/rmgrdesc/replorigindesc.c:19-50