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
  - [FlushDatabaseBuffers](../F/FlushDatabaseBuffers.md)/DropDatabaseBuffers (buffer management)
  - [copydir](../c/copydir.md) (directory copying)
  - [rmtree](../r/rmtree.md) (recursive directory removal)
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

## Simplified Source

```c
void
dbase_redo(XLogReaderState *record)
{
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

    Assert(!XLogRecHasAnyBlockRefs(record));

    if (info == XLOG_DBASE_CREATE_FILE_COPY)
    {
        // Database creation by copying from template
        xl_dbase_create_file_copy_rec *xlrec =
            (xl_dbase_create_file_copy_rec *) XLogRecGetData(record);
        char *src_path, *dst_path, *parent_path;
        struct stat st;

        src_path = GetDatabasePath(xlrec->src_db_id, xlrec->src_tablespace_id);
        dst_path = GetDatabasePath(xlrec->db_id, xlrec->tablespace_id);

        // Force-drop target directory if exists, then recreate
        if (stat(dst_path, &st) == 0 && S_ISDIR(st.st_mode))
            rmtree(dst_path, true);

        // Ensure parent directory exists
        parent_path = pstrdup(dst_path);
        get_parent_directory(parent_path);
        if (stat(parent_path, &st) < 0 && errno == ENOENT)
            recovery_create_dbdir(parent_path, true);

        // Create source directory if missing (recovery scenario)
        if (stat(src_path, &st) < 0 && errno == ENOENT)
            recovery_create_dbdir(src_path, false);

        // Ensure source database is current
        FlushDatabaseBuffers(xlrec->src_db_id);
        WaitForProcSignalBarrier(EmitProcSignalBarrier(PROCSIGNAL_BARRIER_SMGRRELEASE));

        // Copy database directory
        copydir(src_path, dst_path, false);
    }
    else if (info == XLOG_DBASE_CREATE_WAL_LOG)
    {
        // Database creation via WAL logging (empty database)
        xl_dbase_create_wal_log_rec *xlrec =
            (xl_dbase_create_wal_log_rec *) XLogRecGetData(record);
        char *dbpath, *parent_path;

        dbpath = GetDatabasePath(xlrec->db_id, xlrec->tablespace_id);

        // Ensure parent directory exists
        parent_path = pstrdup(dbpath);
        get_parent_directory(parent_path);
        recovery_create_dbdir(parent_path, true);

        // Create database directory with version file
        CreateDirAndVersionFile(dbpath, xlrec->db_id, xlrec->tablespace_id, true);
    }
    else if (info == XLOG_DBASE_DROP)
    {
        // Database drop operation
        xl_dbase_drop_rec *xlrec = (xl_dbase_drop_rec *) XLogRecGetData(record);

        if (InHotStandby)
        {
            // Lock database to prevent reconnections during drop
            LockSharedObjectForSession(DatabaseRelationId, xlrec->db_id, 0, AccessExclusiveLock);
            ResolveRecoveryConflictWithDatabase(xlrec->db_id);
        }

        // Comprehensive cleanup
        ReplicationSlotsDropDBSlots(xlrec->db_id);
        DropDatabaseBuffers(xlrec->db_id);
        ForgetDatabaseSyncRequests(xlrec->db_id);
        XLogDropDatabase(xlrec->db_id);
        WaitForProcSignalBarrier(EmitProcSignalBarrier(PROCSIGNAL_BARRIER_SMGRRELEASE));

        // Remove physical database files from all tablespaces
        for (int i = 0; i < xlrec->ntablespaces; i++)
        {
            char *dst_path = GetDatabasePath(xlrec->db_id, xlrec->tablespace_ids[i]);
            rmtree(dst_path, true);
            pfree(dst_path);
        }

        if (InHotStandby)
            UnlockSharedObjectForSession(DatabaseRelationId, xlrec->db_id, 0, AccessExclusiveLock);
    }
    else
        elog(PANIC, "dbase_redo: unknown op code %u", info);
}
```