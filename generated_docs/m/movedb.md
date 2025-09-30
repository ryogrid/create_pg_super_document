# movedb

## Location
[src/backend/commands/dbcommands.c:1964-2285](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/dbcommands.c#L1964-L2285)

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
  - [copydir](../c/copydir.md): Physically copies database files
  - [XLogBeginInsert](../X/XLogBeginInsert.md)/XLogRegisterData/XLogInsert: WAL logging operations
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md): Updates pg_database tablespace reference
  - [ForceSyncCommit](../F/ForceSyncCommit.md): Ensures synchronous transaction commit
  - [CommitTransactionCommand](../C/CommitTransactionCommand.md)/StartTransactionCommand: Transaction boundaries
  - [rmtree](../r/rmtree.md): Removes old database directory
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

## Simplified Source

```c
static void
movedb(const char *dbname, const char *tblspcname)
{
    Oid db_id, src_tblspcoid, dst_tblspcoid;
    char *src_dbpath, *dst_dbpath;

    // Get database info and acquire exclusive session lock
    if (!get_db_info(dbname, AccessExclusiveLock, &db_id, NULL, NULL, NULL,
                     NULL, NULL, NULL, NULL, &src_tblspcoid, NULL, NULL, NULL, NULL, NULL, NULL))
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_DATABASE),
                        errmsg("database \"%s\" does not exist", dbname)));

    LockSharedObjectForSession(DatabaseRelationId, db_id, 0, AccessExclusiveLock);

    // Validate permissions and constraints
    if (!object_ownercheck(DatabaseRelationId, db_id, GetUserId()))
        aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_DATABASE, dbname);

    if (db_id == MyDatabaseId)
        ereport(ERROR, (errcode(ERRCODE_OBJECT_IN_USE),
                        errmsg("cannot change the tablespace of the currently open database")));

    // Get and validate target tablespace
    dst_tblspcoid = get_tablespace_oid(tblspcname, false);

    if (object_aclcheck(TableSpaceRelationId, dst_tblspcoid, GetUserId(), ACL_CREATE) != ACLCHECK_OK)
        aclcheck_error(ACLCHECK_NO_PRIV, OBJECT_TABLESPACE, tblspcname);

    if (dst_tblspcoid == GLOBALTABLESPACE_OID)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("pg_global cannot be used as default tablespace")));

    // Early return if same tablespace
    if (src_tblspcoid == dst_tblspcoid)
        return;

    // Check for active backends
    if (CountOtherDBBackends(db_id, &notherbackends, &npreparedxacts))
        ereport(ERROR, (errcode(ERRCODE_OBJECT_IN_USE),
                        errmsg("database \"%s\" is being accessed by other users", dbname)));

    // Prepare file paths
    src_dbpath = GetDatabasePath(db_id, src_tblspcoid);
    dst_dbpath = GetDatabasePath(db_id, dst_tblspcoid);

    // Force checkpoint and clear buffers
    RequestCheckpoint(CHECKPOINT_IMMEDIATE | CHECKPOINT_FORCE | CHECKPOINT_WAIT | CHECKPOINT_FLUSH_ALL);
    DropDatabaseBuffers(db_id);

    // Validate target directory is empty
    if (directory_has_files(dst_dbpath))
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                        errmsg("some relations of database \"%s\" are already in tablespace \"%s\"",
                               dbname, tblspcname)));

    // Copy files and update catalog
    PG_ENSURE_ERROR_CLEANUP(movedb_failure_callback, PointerGetDatum(&fparms));
    {
        // Copy database files
        copydir(src_dbpath, dst_dbpath, false);

        // Log file copy operation
        xl_dbase_create_file_copy_rec xlrec;
        xlrec.db_id = db_id;
        xlrec.tablespace_id = dst_tblspcoid;
        xlrec.src_db_id = db_id;
        xlrec.src_tablespace_id = src_tblspcoid;

        XLogBeginInsert();
        XLogRegisterData((char *) &xlrec, sizeof(xl_dbase_create_file_copy_rec));
        XLogInsert(RM_DBASE_ID, XLOG_DBASE_CREATE_FILE_COPY | XLR_SPECIAL_REL_UPDATE);

        // Update pg_database catalog entry
        update_database_tablespace(dbname, dst_tblspcoid);

        // Force checkpoint and sync commit
        RequestCheckpoint(CHECKPOINT_IMMEDIATE | CHECKPOINT_FORCE | CHECKPOINT_WAIT);
        ForceSyncCommit();
    }
    PG_END_ENSURE_ERROR_CLEANUP(movedb_failure_callback, PointerGetDatum(&fparms));

    // Commit transaction and start new one
    CommitTransactionCommand();
    StartTransactionCommand();

    // Remove old files and log removal
    rmtree(src_dbpath, true);

    xl_dbase_drop_rec xlrec2;
    xlrec2.db_id = db_id;
    xlrec2.ntablespaces = 1;
    XLogBeginInsert();
    XLogRegisterData((char *) &xlrec2, sizeof(xl_dbase_drop_rec));
    XLogRegisterData((char *) &src_tblspcoid, sizeof(Oid));
    XLogInsert(RM_DBASE_ID, XLOG_DBASE_DROP | XLR_SPECIAL_REL_UPDATE);

    // Release lock and cleanup
    UnlockSharedObjectForSession(DatabaseRelationId, db_id, 0, AccessExclusiveLock);
    pfree(src_dbpath);
    pfree(dst_dbpath);
}
```