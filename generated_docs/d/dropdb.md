# dropdb

## Location
[src/backend/commands/dbcommands.c:1634-1862](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/dbcommands.c#L1634-L1862)

## Overview
dropdb is the core function that implements the DROP DATABASE SQL command, handling the complete removal of a PostgreSQL database including all its files, metadata, and associated resources.

## Definition

```c
void
dropdb(const char *dbname, bool missing_ok, bool force)
```
## Detailed Description
This comprehensive function performs the complete removal of a PostgreSQL database, including extensive validation, cleanup operations, and filesystem removal. It implements strict safety checks to prevent dropping databases that are in use, contain active connections, or have dependent resources.

The function performs multiple phases: database lookup and locking, permission and safety validation, dependency cleanup, catalog updates with in-place marking as invalid, buffer and synchronization cleanup, checkpoint forcing, and finally filesystem removal. The process is designed to be crash-safe with transactional and non-transactional operations properly sequenced.

Critical safety features include preventing drops of template databases, the currently connected database, databases with active backends (unless forced), databases with active replication slots, and databases with logical replication subscriptions.

## Parameters / Member Variables
- : Name of the database to be dropped
- : If true, don't error when database doesn't exist, just issue a notice
- : If true, terminate existing connections to the database before dropping

## Dependencies
- Functions called/Symbols referenced:
  - [get_db_info](../g/get_db_info.md), CountOtherDBBackends, TerminateOtherDBBackends
  - [ReplicationSlotsCountDBSlots](../R/ReplicationSlotsCountDBSlots.md), ReplicationSlotsDropDBSlots
  - [CountDBSubscriptions](../C/CountDBSubscriptions.md)
  - [DeleteSharedComments](../D/DeleteSharedComments.md), DeleteSharedSecurityLabel, DropSetting
  - [dropDatabaseDependencies](dropDatabaseDependencies.md), pgstat_drop_database
  - [systable_inplace_update_begin](../s/systable_inplace_update_begin.md), systable_inplace_update_finish
  - [DropDatabaseBuffers](../D/DropDatabaseBuffers.md), ForgetDatabaseSyncRequests
  - [RequestCheckpoint](../R/RequestCheckpoint.md), ForceSyncCommit
  - [remove_dbtablespaces](../r/remove_dbtablespaces.md)
- Called from (representative examples):
  - [DropDatabase](../D/DropDatabase.md) (SQL command processing wrapper)

## Notes and Other Information
- Uses AccessExclusiveLock to prevent concurrent access during the entire drop operation
- Implements in-place update to mark database as invalid (datconnlimit = DATCONNLIMIT_INVALID_DB) before filesystem operations
- Forces WAL flush after marking invalid to ensure durability before irreversible filesystem operations
- Includes comprehensive cleanup of shared buffers, sync requests, and tablespace directories
- Uses process signal barriers to ensure all backends close file handles before filesystem removal
- Forces synchronous commit to minimize the window between filesystem removal and transaction commit
- Cannot drop template databases, the current database, databases with active backends (unless forced), databases with active replication slots, or databases with subscriptions

## Simplified Source

```c
void dropdb(const char *dbname, bool missing_ok, bool force) {
    Oid db_id;
    bool db_istemplate;
    Relation pgdbrel;

    // Step 1: Lock database catalog and look up target database
    pgdbrel = table_open(DatabaseRelationId, RowExclusiveLock);

    if (!get_db_info(dbname, AccessExclusiveLock, &db_id, NULL, NULL,
                     &db_istemplate, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)) {
        if (!missing_ok) {
            ereport(ERROR, (errcode(ERRCODE_UNDEFINED_DATABASE),
                           errmsg("database \"%s\" does not exist", dbname)));
        } else {
            // Database doesn't exist, just issue notice and return
            table_close(pgdbrel, RowExclusiveLock);
            ereport(NOTICE, (errmsg("database \"%s\" does not exist, skipping", dbname)));
            return;
        }
    }

    // Step 2: Validate permissions and safety constraints
    if (!object_ownercheck(DatabaseRelationId, db_id, GetUserId()))
        aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_DATABASE, dbname);

    if (db_istemplate)
        ereport(ERROR, (errmsg("cannot drop a template database")));

    if (db_id == MyDatabaseId)
        ereport(ERROR, (errmsg("cannot drop the currently open database")));

    // Step 3: Check for active connections and dependencies
    int nslots, nslots_active;
    ReplicationSlotsCountDBSlots(db_id, &nslots, &nslots_active);
    if (nslots_active)
        ereport(ERROR, (errmsg("database is used by active logical replication slot")));

    int nsubscriptions = CountDBSubscriptions(db_id);
    if (nsubscriptions > 0)
        ereport(ERROR, (errmsg("database is being used by logical replication subscription")));

    // Step 4: Terminate connections if forced
    if (force)
        TerminateOtherDBBackends(db_id);

    int notherbackends, npreparedxacts;
    if (CountOtherDBBackends(db_id, &notherbackends, &npreparedxacts))
        ereport(ERROR, (errmsg("database is being accessed by other users")));

    // Step 5: Clean up metadata and dependencies
    DeleteSharedComments(db_id, DatabaseRelationId);
    DeleteSharedSecurityLabel(db_id, DatabaseRelationId);
    DropSetting(db_id, InvalidOid);
    dropDatabaseDependencies(db_id);
    pgstat_drop_database(db_id);

    // Step 6: Mark database as invalid and delete catalog entry
    // In-place update to mark as invalid before filesystem operations
    mark_database_invalid(pgdbrel, dbname, db_id);
    XLogFlush(XactLastRecEnd);  // Ensure durability

    // Delete the catalog tuple
    delete_database_tuple(pgdbrel, dbname);

    // Step 7: Clean up resources and filesystem
    ReplicationSlotsDropDBSlots(db_id);
    DropDatabaseBuffers(db_id);
    ForgetDatabaseSyncRequests(db_id);

    // Force checkpoint and wait for all backends to release file handles
    RequestCheckpoint(CHECKPOINT_IMMEDIATE | CHECKPOINT_FORCE | CHECKPOINT_WAIT);
    WaitForProcSignalBarrier(EmitProcSignalBarrier(PROCSIGNAL_BARRIER_SMGRRELEASE));

    // Remove database files from all tablespaces
    remove_dbtablespaces(db_id);

    // Close catalog and force synchronous commit
    table_close(pgdbrel, NoLock);
    ForceSyncCommit();
}
```