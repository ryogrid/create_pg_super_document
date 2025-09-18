# createdb_failure_callback

## Location
src/backend/commands/dbcommands.c: 1595 - 1633

## Overview
createdb_failure_callback is an error cleanup callback function that handles rollback operations when database creation fails, ensuring proper resource cleanup and lock release.

## Definition


## Detailed Description
This callback function is registered with the PostgreSQL error handling system to perform necessary cleanup operations when CREATE DATABASE operations fail after partial completion. It handles different cleanup scenarios based on the database creation strategy used.

For WAL_LOG strategy failures, it drops any database pages that may have been copied to shared buffers, cancels pending fsync and unlink requests, and releases locks on the destination database. For both strategies, it releases locks on the source (template) database and removes any successfully copied subdirectories and files.

The function uses the createdb_failure_params structure to access context information including source and destination database OIDs and the creation strategy used.

## Parameters / Member Variables
- : Error code indicating the type of failure (standard PostgreSQL error callback parameter)
- : Datum containing pointer to createdb_failure_params structure with cleanup context information
  - : OID of the source (template) database  
  - : OID of the destination database being created
  - : Database creation strategy (CREATEDB_WAL_LOG or CREATEDB_FILE_COPY)

## Dependencies
- Functions called/Symbols referenced:
  - [DropDatabaseBuffers](../D/DropDatabaseBuffers.md)
  - [ForgetDatabaseSyncRequests](../F/ForgetDatabaseSyncRequests.md)  
  - [UnlockSharedObject](../U/UnlockSharedObject.md)
  - [remove_dbtablespaces](../r/remove_dbtablespaces.md)
  - createdb_failure_params (structure)
- Called from (representative examples):
  - [createdb](createdb.md) (registered as error callback via PG_ENSURE_ERROR_CLEANUP)

## Notes and Other Information
- Only performs buffer and sync request cleanup for CREATEDB_WAL_LOG strategy since FILE_COPY strategy doesn't use shared buffers
- Does not call pgstat_drop_database since the database creation never completed, so no statistics exist
- Lock release order is important: destination database lock first (if applicable), then source database lock  
- Uses remove_dbtablespaces to clean up any partially created database directory structures
- Registered and called automatically by PostgreSQL's error handling mechanism when exceptions occur during database creation