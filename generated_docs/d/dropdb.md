# dropdb

## Location
src/backend/commands/dbcommands.c: 1634 - 1862

## Overview
dropdb is the core function that implements the DROP DATABASE SQL command, handling the complete removal of a PostgreSQL database including all its files, metadata, and associated resources.

## Definition


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
  - get_db_info, CountOtherDBBackends, TerminateOtherDBBackends
  - ReplicationSlotsCountDBSlots, ReplicationSlotsDropDBSlots
  - CountDBSubscriptions
  - DeleteSharedComments, DeleteSharedSecurityLabel, DropSetting
  - dropDatabaseDependencies, pgstat_drop_database
  - systable_inplace_update_begin, systable_inplace_update_finish
  - DropDatabaseBuffers, ForgetDatabaseSyncRequests
  - RequestCheckpoint, ForceSyncCommit
  - remove_dbtablespaces
- Called from (representative examples):
  - DropDatabase (SQL command processing wrapper)

## Notes and Other Information
- Uses AccessExclusiveLock to prevent concurrent access during the entire drop operation
- Implements in-place update to mark database as invalid (datconnlimit = DATCONNLIMIT_INVALID_DB) before filesystem operations
- Forces WAL flush after marking invalid to ensure durability before irreversible filesystem operations
- Includes comprehensive cleanup of shared buffers, sync requests, and tablespace directories
- Uses process signal barriers to ensure all backends close file handles before filesystem removal
- Forces synchronous commit to minimize the window between filesystem removal and transaction commit
- Cannot drop template databases, the current database, databases with active backends (unless forced), databases with active replication slots, or databases with subscriptions