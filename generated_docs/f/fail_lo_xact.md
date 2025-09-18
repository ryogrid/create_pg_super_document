# fail_lo_xact

## Location
[src/bin/psql/large_obj.c:121-141](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/large_obj.c#L121-L141)

## Overview
Handles cleanup after failed large object operations by rolling back transactions that were started by the operation, ensuring database consistency when operations fail.

## Definition
```c
static bool fail_lo_xact(const char *operation, bool own_transaction)
```

## Detailed Description
This function serves as the error handling counterpart to both start_lo_xact and finish_lo_xact, managing transaction cleanup when large object operations fail. It rolls back transactions that were explicitly started for the large object operation, but only touches transactions that it owns (as indicated by the own_transaction parameter). The function respects psql's autocommit setting and always returns false to propagate the failure condition to the calling code. This ensures that partial changes from failed large object operations are not committed to the database.

## Parameters / Member Variables
- `operation`: String describing the operation that failed (used for context, though not directly used in current implementation)
- `own_transaction`: Boolean indicating whether this function should rollback the transaction (true if start_lo_xact started it)

## Dependencies
- Functions called/Symbols referenced:
  - [PSQLexec](../P/PSQLexec.md) (psql utility function to execute SQL commands)
  - [PQclear](../P/PQclear.md) (libpq function to free result memory)
  - pset.autocommit (global setting controlling automatic transaction commits)
- Called from (representative examples):
  - [do_lo_export](../d/do_lo_export.md)
  - [do_lo_import](../d/do_lo_import.md) (multiple error paths)
  - [do_lo_unlink](../d/do_lo_unlink.md)

## Notes and Other Information
- Always returns false to indicate operation failure
- Only rolls back transactions when both own_transaction is true AND autocommit is enabled
- Part of the transaction management trilogy with start_lo_xact and finish_lo_xact
- Critical for maintaining database consistency by preventing partial commits of failed operations
- Static function with scope limited to large_obj.c file
- Simple implementation that doesn't check rollback success, focusing on cleanup rather than error reporting