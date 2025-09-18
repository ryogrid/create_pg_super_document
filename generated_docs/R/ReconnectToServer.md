# ReconnectToServer

## Location
src/bin/pg_dump/pg_backup_db.c: 74 - 109

## Overview
Safely reconnects to a PostgreSQL server during pg_dump/pg_restore operations, optionally switching to a different database.

## Definition
```c
void ReconnectToServer(ArchiveHandle *AH, const char *dbname)
```

## Detailed Description
This function handles reconnection to a PostgreSQL server during dump or restore operations. It safely manages the transition from an old connection to a new one, ensuring that signal handlers (particularly SIGINT) don't attempt to access a dead connection during the transition. The function can optionally switch to a different database by specifying the dbname parameter. The new database name is saved in override_dbname to affect future reconnection attempts as well.

## Parameters / Member Variables
- `AH`: Archive handle containing the current connection and configuration
- `dbname`: Optional database name to connect to; if NULL, uses the database associated with the archive handle

## Dependencies
- Functions called/Symbols referenced:
  - [pg_strdup](../p/pg_strdup.md) (duplicates database name string)
  - [ConnectDatabase](../C/ConnectDatabase.md) (establishes new database connection)
  - [PQfinish](../P/PQfinish.md) (closes old PostgreSQL connection)
- Called from (representative examples):
  - [_reconnectToDB](../r/_reconnectToDB.md)
  - appendByteaLiteralAHX

## Notes and Other Information
- Safely handles connection transitions by establishing the new connection before closing the old one
- Updates ArchiveHandle's connCancel before closing old connection to prevent race conditions with signal handlers
- Stores override_dbname in RestoreOptions to persist database preference across reconnections
- Temporarily sets AH->connection to NULL to bypass error checks in ConnectDatabase
- Critical for operations that require switching databases or recovering from connection failures during lengthy dump/restore processes