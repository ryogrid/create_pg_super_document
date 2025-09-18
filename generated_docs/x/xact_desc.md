# xact_desc

## Location
src/backend/access/rmgrdesc/xactdesc.c: 438 - 485

## Overview
Generates human-readable descriptions of transaction-related WAL (Write-Ahead Logging) records for debugging and monitoring purposes.

## Definition
```c
void xact_desc(StringInfo buf, XLogReaderState *record)
```

## Detailed Description
The `xact_desc` function is part of PostgreSQL's resource manager description interface, specifically designed to format transaction WAL records into readable text. It examines the operation type of a transaction WAL record and dispatches to appropriate specialized description functions based on the transaction operation being performed.

The function handles all major transaction operations including:
- Transaction commits (both regular and prepared)
- Transaction aborts (both regular and prepared)  
- Two-phase commit preparations
- Transaction ID assignments for subtransactions
- Cache invalidation messages

This function is primarily used by PostgreSQL's WAL inspection tools and logging mechanisms to provide meaningful descriptions of transaction operations recorded in the WAL stream.

## Parameters
- `buf`: A StringInfo buffer where the formatted description will be appended
- `record`: An XLogReaderState containing the WAL record data to be described

## Dependencies
- Functions called/Symbols referenced:
  - XLogRecGetData
  - XLogRecGetInfo  
  - XLogRecGetOrigin
  - [xact_desc_commit](xact_desc_commit.md)
  - [xact_desc_abort](xact_desc_abort.md)
  - [xact_desc_prepare](xact_desc_prepare.md)
  - [xact_desc_assignment](xact_desc_assignment.md)
  - [standby_desc_invalidations](../s/standby_desc_invalidations.md)
  - appendStringInfo
- Called from (representative examples):
  - WAL description framework (no direct references found in current analysis)

## Notes and Other Information
- The function uses a bitmask (XLOG_XACT_OPMASK) to extract the operation type from the WAL record info
- For assignment records, it specifically notes that it ignores the WAL record's XID in favor of the top-level transaction ID
- Cache invalidation handling delegates to the shared `standby_desc_invalidations` utility function
- Each transaction operation type has its own specialized description helper function
- Located in src/backend/access/rmgrdesc/xactdesc.c:438-485