# pg_export_snapshot

## Location
[src/backend/utils/time/snapmgr.c:1272-1286](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/time/snapmgr.c#L1272-L1286)

## Overview
pg_export_snapshot is a SQL-callable function that provides a user interface to export the current transaction snapshot, returning a snapshot identifier that can be used to import the snapshot in other sessions.

## Definition
```c
Datum pg_export_snapshot(PG_FUNCTION_ARGS)
```

## Detailed Description
pg_export_snapshot serves as a PostgreSQL SQL function wrapper around the internal ExportSnapshot function. It exports the currently active snapshot of the calling transaction and returns the snapshot file name as a text value that can be used with SET TRANSACTION SNAPSHOT to import the snapshot in other database sessions.

The function is typically used in scenarios where multiple database sessions need to see a consistent view of the database at a specific point in time, such as:
- Parallel query processing across multiple connections
- Coordinated data export operations
- Ensuring consistency across distributed operations

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - [GetActiveSnapshot](../G/GetActiveSnapshot.md)
  - [ExportSnapshot](../E/ExportSnapshot.md)
  - [cstring_to_text](../c/cstring_to_text.md)
  - PG_RETURN_TEXT_P
- Called from (representative examples):
  - SQL commands (no direct C callers found)

## Notes and Other Information
- This function is exposed as a PostgreSQL built-in function accessible via SQL
- Returns a text value representing the snapshot identifier (file name)
- Uses the active snapshot of the current transaction via GetActiveSnapshot()
- The returned snapshot identifier can be used with SET TRANSACTION SNAPSHOT command
- Inherits all restrictions from ExportSnapshot (e.g., cannot be called from subtransactions)
- The function follows PostgreSQLs standard function calling convention using PG_FUNCTION_ARGS