# DisconnectDatabase

## Location
[src/bin/pg_dump/pg_backup_db.c:225-253](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_backup_db.c#L225-L253)

## Overview
Cleanly closes a PostgreSQL database connection while properly handling active queries and signal cancellation.

## Definition
```c
void DisconnectDatabase(Archive *AHX)
```

## Detailed Description
This function safely disconnects from a PostgreSQL database by performing proper cleanup of active connections. It first checks if there are any active queries running and attempts to cancel them before closing the connection. The function also ensures that signal handlers are properly deregistered to prevent race conditions where a signal might try to operate on a closed connection. This careful cleanup process is particularly important during error conditions or when pg_fatal() is called.

## Parameters / Member Variables
- `AHX`: Archive handle containing the database connection to be closed

## Dependencies
- Functions called/Symbols referenced:
  - [PQtransactionStatus](../P/PQtransactionStatus.md) (checks if there are active queries)
  - [PQcancel](../P/PQcancel.md) (cancels active queries)
  - [set_archive_cancel_info](../s/set_archive_cancel_info.md) (deregisters signal handlers)
  - [PQfinish](../P/PQfinish.md) (closes the PostgreSQL connection)
- Called from (representative examples):
  - [archive_close_connection](../a/archive_close_connection.md)
  - [RunWorker](../R/RunWorker.md)
  - [RestoreArchive](../R/RestoreArchive.md)
  - [restore_toc_entries_prefork](../r/restore_toc_entries_prefork.md)

## Notes and Other Information
- Safely handles cases where connection is already NULL by returning early
- Attempts query cancellation only if there's an active transaction (PQTRANS_ACTIVE)
- Ignores errors from PQcancel as the connection is being closed anyway
- Sets AH->connection to NULL after closing to prevent double-close issues
- Critical for preventing resource leaks and ensuring clean shutdown in both normal and error conditions
- Used extensively in parallel processing scenarios where worker connections need proper cleanup