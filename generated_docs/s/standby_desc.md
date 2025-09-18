# standby_desc

## Location
[src/backend/access/rmgrdesc/standbydesc.c:47-78](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/rmgrdesc/standbydesc.c#L47-L78)

## Overview
Main entry point function for formatting standby-related WAL record descriptions into human-readable strings for debugging and monitoring purposes.

## Definition


## Detailed Description
This function serves as the primary dispatcher for describing different types of standby-related WAL records. It examines the WAL record type and delegates to appropriate specialized description functions. The function handles three main types of standby WAL records:

1. **XLOG_STANDBY_LOCK**: Describes standby lock records, showing transaction ID, database OID, and relation OID for each lock
2. **XLOG_RUNNING_XACTS**: Delegates to standby_desc_running_xacts() to describe running transaction information
3. **XLOG_INVALIDATIONS**: Delegates to standby_desc_invalidations() to describe cache invalidation records

This function is part of PostgreSQL's WAL record description infrastructure, used for debugging, monitoring, and understanding replication activities.

## Parameters / Member Variables
- `buf`: StringInfo buffer to append the formatted description to
- `record`: XLogReaderState pointer containing the WAL record to be described

## Dependencies
- Functions called/Symbols referenced:
  - XLogRecGetData
  - XLogRecGetInfo
  - XLR_INFO_MASK
  - appendStringInfo
  - [standby_desc_running_xacts](standby_desc_running_xacts.md)
  - [standby_desc_invalidations](standby_desc_invalidations.md)
  - xl_standby_locks (struct type)
  - xl_running_xacts (struct type)
  - xl_invalidations (struct type)
- Called from (representative examples):
  - WAL record description infrastructure (referenced in standbydefs.h)

## Notes and Other Information
- This function is the main public interface for standby WAL record descriptions
- Uses a switch-like structure based on WAL record info flags to determine record type
- Handles multiple lock entries within a single XLOG_STANDBY_LOCK record
- Part of the resource manager description system in PostgreSQL
- Essential for debugging replication and standby server operations