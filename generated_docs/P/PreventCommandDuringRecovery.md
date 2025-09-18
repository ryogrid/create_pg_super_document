# PreventCommandDuringRecovery

## Location
src/backend/tcop/utility.c: 441 - 458

## Overview
PreventCommandDuringRecovery throws an error if the database is in recovery mode, specifically targeting commands that are allowed in read-only transactions but incompatible with Hot Standby operation.

## Definition
`void PreventCommandDuringRecovery(const char *cmdname)`

## Detailed Description
This function provides specialized protection for commands that fall into a specific category: operations that pass read-only transaction checks but are still unsafe during database recovery (Hot Standby mode). While most unsafe operations in Hot Standby are caught by XactReadOnly tests, certain commands require this additional layer of protection.

The function uses RecoveryInProgress() to determine if the database is currently in recovery mode and generates a standardized error message if recovery is active. This addresses the gap between read-only transaction restrictions and Hot Standby operational limitations, ensuring that commands requiring WAL writing or other recovery-incompatible operations are properly rejected.

## Parameters / Member Variables
- `cmdname`: String containing the name of the SQL command being executed (e.g., "NOTIFY", "LISTEN") for inclusion in the error message

## Dependencies
- Functions called/Symbols referenced:
  - RecoveryInProgress (function to check if database is in recovery mode)
  - ereport (error reporting mechanism)
  - errcode, errmsg (error handling macros)
- Called from (representative examples):
  - pg_notify (src/backend/commands/async.c:573)
  - standard_ProcessUtility (src/backend/tcop/utility.c:582)
  - pg_current_xact_id (src/backend/utils/adt/xid8funcs.c:342)

## Notes and Other Information
- Uses ERRCODE_READ_ONLY_SQL_TRANSACTION error code, consistent with read-only restrictions
- Primarily used for notification system operations and transaction ID functions that require XID assignment
- Essential component of Hot Standby safety mechanisms in PostgreSQL replication
- Complements the related processed symbol RecoveryInProgress by providing the enforcement layer for recovery-mode restrictions
- Bridges the gap between read-only transaction checks and Hot Standby operational requirements