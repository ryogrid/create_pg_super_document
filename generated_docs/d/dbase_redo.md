# dbase_redo

## Location
[src/backend/commands/dbcommands.c:3270-3431](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/dbcommands.c#L3270-L3431)

## Overview
The WAL replay function for database-related operations that handles the redo of database creation and dropping during recovery.

## Definition

```c
struct stat st;
```
## Detailed Description
This function is the central WAL replay handler for database operations during PostgreSQL recovery. It processes three types of database-related WAL records: file-copy database creation (XLOG_DBASE_CREATE_FILE_COPY), WAL-logged database creation (XLOG_DBASE_CREATE_WAL_LOG), and database dropping (XLOG_DBASE_DROP). For database creation, it handles both copying from template databases and creating new empty databases, ensuring proper directory structure exists and managing potential missing tablespace scenarios during recovery. For database dropping, it performs comprehensive cleanup including buffer management, replication slot cleanup, and physical file removal while handling hot standby locking concerns.

## Parameters / Member Variables
- : XLogReaderState pointer containing the WAL record to be replayed

## Dependencies
- Functions called/Symbols referenced:
  - XLogRecGetInfo (WAL record info extraction)
  - XLogRecGetData (WAL record data extraction)  
  - XLogRecHasAnyBlockRefs (block reference checking)
  - [GetDatabasePath](../G/GetDatabasePath.md) (database path construction)
  - [recovery_create_dbdir](../r/recovery_create_dbdir.md) (missing directory creation)
  - FlushDatabaseBuffers/DropDatabaseBuffers (buffer management)
  - copydir (directory copying)
  - rmtree (recursive directory removal)
  - [CreateDirAndVersionFile](../C/CreateDirAndVersionFile.md) (database directory setup)
  - [ReplicationSlotsDropDBSlots](../R/ReplicationSlotsDropDBSlots.md) (replication cleanup)
  - [LockSharedObjectForSession](../L/LockSharedObjectForSession.md)/UnlockSharedObjectForSession (hot standby locking)
  - [ResolveRecoveryConflictWithDatabase](../R/ResolveRecoveryConflictWithDatabase.md) (conflict resolution)
  - [EmitProcSignalBarrier](../E/EmitProcSignalBarrier.md)/WaitForProcSignalBarrier (backend coordination)
- Called from (representative examples):
  - WAL resource manager framework during recovery

## Notes and Other Information
- Central component of PostgreSQL's database crash recovery system
- Handles three distinct database operation types with different recovery strategies
- Implements force-drop-and-recreate strategy for database creation replay for simplicity
- Manages complex hot standby scenarios with proper locking to prevent connection conflicts
- Uses recovery_create_dbdir to handle missing tablespace directories during recovery
- Performs comprehensive cleanup during database drops including buffers, sync requests, and replication slots
- Critical for maintaining database consistency across crashes and restarts
- Part of PostgreSQL's resource manager framework for WAL replay