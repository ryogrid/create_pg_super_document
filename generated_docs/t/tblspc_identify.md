# tblspc_identify

## Location
[src/backend/access/rmgrdesc/tblspcdesc.c:41-56](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/rmgrdesc/tblspcdesc.c#L41-L56)

## Overview
Returns a human-readable string identifier for tablespace-related Write-Ahead Logging (WAL) record types, used for debugging and logging purposes in PostgreSQL's WAL system.

## Definition
```c
const char *tblspc_identify(uint8 info)
```

## Detailed Description
The `tblspc_identify` function is part of PostgreSQL's resource manager descriptor system for tablespace operations. It takes a WAL record info byte and returns a corresponding string description of the tablespace operation type. This function is primarily used by PostgreSQL's WAL debugging and analysis tools to provide human-readable descriptions of WAL records related to tablespace operations.

The function masks out the `XLR_INFO_MASK` bits from the input to isolate the actual operation type, then matches against known tablespace WAL record types. It supports two main tablespace operations: CREATE and DROP.

This function is part of the rmgr (resource manager) descriptor infrastructure that helps with WAL record interpretation and debugging throughout the PostgreSQL system.

## Parameters / Member Variables
- `info`: An 8-bit unsigned integer containing the WAL record info byte, which includes both the operation type and additional flags that are masked out during processing

## Dependencies
- Functions called/Symbols referenced:
  - XLR_INFO_MASK (constant used to mask out flag bits)
  - XLOG_TBLSPC_CREATE (constant for tablespace creation operations)
  - XLOG_TBLSPC_DROP (constant for tablespace drop operations)
- Called from (representative examples):
  - No direct references found in the codebase (likely called through function pointer in rmgr descriptor table)

## Notes and Other Information
- Located in `src/backend/access/rmgrdesc/tblspcdesc.c:41-56`
- Returns NULL for unrecognized operation types
- Part of the tablespace resource manager descriptor system alongside `tblspc_desc()` and `tblspc_redo()`
- The function is declared in `src/include/commands/tablespace.h:67`
- Used primarily for WAL analysis, debugging, and logging rather than core database operations
- The masking operation `info & ~XLR_INFO_MASK` removes auxiliary flags to isolate the core operation type