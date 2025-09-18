# do_lo_unlink

## Location
src/bin/psql/large_obj.c: 239 - 264

## Overview
Removes a large object from the PostgreSQL database by its OID.

## Definition
```c
bool do_lo_unlink(const char *loid_arg)
```

## Detailed Description
The `do_lo_unlink` function implements the PostgreSQL \lo_unlink command functionality in psql. It deletes a large object from the database by converting the provided OID string to an actual OID and calling the libpq lo_unlink function. The function manages transaction boundaries automatically and provides proper error handling throughout the deletion process. The operation is performed within a transaction context to ensure data consistency.

## Parameters / Member Variables
- `loid_arg`: String representation of the large object OID to be deleted from the database

## Dependencies
- Functions called/Symbols referenced:
  - atooid (string to OID conversion)
  - [start_lo_xact](../s/start_lo_xact.md) (transaction management)
  - [SetCancelConn](../S/SetCancelConn.md)/ResetCancelConn (cancellation handling)
  - [lo_unlink](../l/lo_unlink.md) (libpq large object deletion function)
  - pg_log_info (error logging)
  - [fail_lo_xact](../f/fail_lo_xact.md) (transaction rollback)
  - [finish_lo_xact](../f/finish_lo_xact.md) (transaction commit)
  - [print_lo_result](../p/print_lo_result.md) (result output)
- Called from (representative examples):
  - [exec_command_lo](../e/exec_command_lo.md) (psql command execution)

## Notes and Other Information
- Returns true on success, false on failure
- Automatically manages transaction boundaries with start_lo_xact/finish_lo_xact
- Converts string OID argument to actual OID using atooid function
- Provides proper error handling and cleanup on failure
- Uses lo_unlink return status of -1 to detect errors
- Part of psql's large object management subsystem
- Permanently removes the large object and all its associated data from the database