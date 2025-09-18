# xact_identify

## Location
[src/backend/access/rmgrdesc/xactdesc.c:486-516](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/rmgrdesc/xactdesc.c#L486-L516)

## Overview
Returns a string identifier for transaction WAL record operation types, providing human-readable names for different transaction operations.

## Definition
```c
const char *xact_identify(uint8 info)
```

## Detailed Description
The `xact_identify` function serves as a utility to convert numeric transaction operation codes into their corresponding string representations. It examines the operation mask from a transaction WAL record's info field and returns the appropriate operation name as a constant string.

This function is part of PostgreSQL's WAL record description infrastructure and is typically used alongside `xact_desc` to provide meaningful operation names in WAL analysis tools, logging output, and debugging interfaces.

The function handles all transaction operation types defined in the transaction resource manager:
- COMMIT: Regular transaction commit
- PREPARE: Two-phase commit preparation  
- ABORT: Transaction abort/rollback
- COMMIT_PREPARED: Commit of a previously prepared transaction
- ABORT_PREPARED: Abort of a previously prepared transaction
- ASSIGNMENT: Subtransaction ID assignment
- INVALIDATION: Cache invalidation messages

## Parameters
- `info`: The info field from a transaction WAL record, containing operation type and flags

## Dependencies  
- Functions called/Symbols referenced:
  - XLOG_XACT_OPMASK (bitmask constant)
  - XLOG_XACT_COMMIT
  - XLOG_XACT_PREPARE
  - XLOG_XACT_ABORT
  - XLOG_XACT_COMMIT_PREPARED
  - XLOG_XACT_ABORT_PREPARED
  - XLOG_XACT_ASSIGNMENT
  - XLOG_XACT_INVALIDATIONS
- Called from (representative examples):
  - WAL description framework (no direct references found in current analysis)

## Notes and Other Information
- Returns NULL for unrecognized operation types
- Uses XLOG_XACT_OPMASK to extract only the operation bits from the info parameter
- All returned strings are compile-time constants, so no memory management is required
- Commonly used in conjunction with xact_desc for comprehensive WAL record descriptions
- Located in src/backend/access/rmgrdesc/xactdesc.c:486-516