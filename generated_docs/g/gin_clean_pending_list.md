# gin_clean_pending_list

## Location
src/backend/access/gin/ginfast.c: 1031 - 1091

## Overview
SQL-callable function that allows users to manually clean the insert pending list of a GIN index, transferring pending entries to the main index structure.

## Definition
```c
Datum gin_clean_pending_list(PG_FUNCTION_ARGS)
```

## Detailed Description
This function provides a SQL interface for manually triggering cleanup of a GIN index's pending list. It performs comprehensive validation to ensure the operation is safe and permitted, including checking that recovery is not in progress, verifying the target is a valid GIN index, ensuring proper ownership permissions, and confirming the index is in a valid state. When all conditions are met, it calls ginInsertCleanup to perform the actual cleanup operation and returns the number of pages deleted during the process.

## Parameters / Member Variables
- Uses PostgreSQL's function argument system (PG_FUNCTION_ARGS):
  - Argument 0: OID of the index to clean (extracted via PG_GETARG_OID(0))

## Dependencies
- Functions called/Symbols referenced:
  - [index_open](../i/index_open.md), index_close (index access functions)
  - [RecoveryInProgress](../R/RecoveryInProgress.md) (check if database recovery is active)
  - [object_ownercheck](../o/object_ownercheck.md) (verify user ownership permissions)
  - [aclcheck_error](../a/aclcheck_error.md) (report access control errors)
  - RELATION_IS_OTHER_TEMP (check for temporary relations from other sessions)
  - [initGinState](../i/initGinState.md) (initialize GIN state structure)
  - [ginInsertCleanup](ginInsertCleanup.md) (perform the actual cleanup operation)
  - PG_RETURN_INT64 (return 64-bit integer result)
- Called from (representative examples):
  - SQL interface: `SELECT gin_clean_pending_list('index_name'::regclass);`

## Notes and Other Information
- This is a public SQL-callable function, accessible via SQL commands
- Requires RowExclusiveLock on the target index to prevent concurrent modifications
- Performs extensive validation before attempting cleanup:
  - Prevents operation during database recovery
  - Verifies the target is actually a GIN index
  - Blocks access to temporary indexes from other sessions
  - Requires index ownership (similar to VACUUM privileges)
- Only processes valid indexes (indisvalid=true), skipping invalid ones with a debug message
- Returns the number of deleted pages as a result
- Uses forceCleanup=true, full_clean=true, and fill_fsm=true when calling ginInsertCleanup
- Handles error reporting with appropriate error codes and user-friendly messages
- Comparable to VACUUM in terms of required privileges and system impact
- Useful for manual maintenance when automatic cleanup is insufficient