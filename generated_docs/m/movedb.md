# movedb

## Location
src/backend/commands/dbcommands.c: 1964 - 2285

## Overview
movedb implements the core functionality of ALTER DATABASE SET TABLESPACE by physically moving database files from one tablespace to another while ensuring data consistency and proper transaction handling.

## Definition
```c
static void movedb(const char *dbname, const char *tblspcname)
```

## Detailed Description
movedb performs a complete database tablespace relocation operation involving file system operations, catalog updates, and WAL logging. The function first validates permissions and ensures no active sessions are using the database, then creates a checkpoint to flush all buffers, copies the entire database directory to the new tablespace location, updates the pg_database catalog entry, and finally removes the old files. The operation uses an error cleanup callback to handle partial failures and ensure consistency even if errors occur during the process.

## Parameters / Member Variables
- `dbname`: The name of the database to move to a different tablespace
- `tblspcname`: The name of the target tablespace where the database will be moved

## Dependencies
- Functions called/Symbols referenced:
  - [get_db_info](../g/get_db_info.md): Retrieves database information and locks
  - [LockSharedObjectForSession](../L/LockSharedObjectForSession.md): Acquires session-level exclusive lock
  - [object_ownercheck](../o/object_ownercheck.md): Validates database ownership
  - [get_tablespace_oid](../g/get_tablespace_oid.md): Resolves tablespace name to OID
  - [object_aclcheck](../o/object_aclcheck.md): Checks tablespace CREATE permissions
  - [CountOtherDBBackends](../C/CountOtherDBBackends.md): Ensures no active database sessions
  - [GetDatabasePath](../G/GetDatabasePath.md): Constructs source and destination paths
  - [RequestCheckpoint](../R/RequestCheckpoint.md): Forces checkpoint for consistency
  - [DropDatabaseBuffers](../D/DropDatabaseBuffers.md): Clears database buffers from shared memory
  - copydir: Physically copies database files
  - [XLogBeginInsert](../X/XLogBeginInsert.md)/XLogRegisterData/XLogInsert: WAL logging operations
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md): Updates pg_database tablespace reference
  - [ForceSyncCommit](../F/ForceSyncCommit.md): Ensures synchronous transaction commit
  - [CommitTransactionCommand](../C/CommitTransactionCommand.md)/StartTransactionCommand: Transaction boundaries
  - rmtree: Removes old database directory
- Called from (representative examples):
  - [AlterDatabase](../A/AlterDatabase.md): Database alteration command handler

## Notes and Other Information
- Uses session-level locking to prevent concurrent operations during the move
- Performs two checkpoints: one before copying to ensure source consistency, another after catalog update to minimize WAL replay risks
- Implements error cleanup via movedb_failure_callback to remove partial copies
- Cannot move the currently connected database
- Validates that target tablespace doesn't already contain database objects
- Logs both file copy and directory removal operations to WAL for crash recovery
- Splits operation across transaction boundaries to minimize lock duration while maintaining consistency