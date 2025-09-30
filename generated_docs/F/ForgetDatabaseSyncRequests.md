# ForgetDatabaseSyncRequests

## Location
[src/backend/storage/smgr/md.c:1430-1447](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/smgr/md.c#L1430-L1447)

## Overview
Cancels all pending fsync and unlink requests for an entire database by registering a filter request that removes database-specific sync operations.

## Definition
```c
void ForgetDatabaseSyncRequests(Oid dbid)
```

## Detailed Description
The ForgetDatabaseSyncRequests function is used to cancel all pending sync requests (both fsync and unlink operations) for an entire database. This is typically called during database drop operations or when recovering from database creation failures.

When a database is being dropped or when database creation fails, all pending sync operations for that database become unnecessary and should be removed from the pending operations queue. This function uses a filter-based approach to efficiently remove all sync requests related to the specified database.

The function creates a FileTag with the database OID and uses special marker values (InvalidForkNumber and InvalidBlockNumber) to indicate that this is a database-wide operation. It then registers a SYNC_FILTER_REQUEST that instructs the sync system to remove all pending operations matching the database OID.

This bulk cancellation approach is much more efficient than individually canceling sync requests for each relation in the database.

## Parameters / Member Variables
- `dbid`: Oid (Object identifier) of the database for which to cancel all sync requests

## Dependencies
- Functions called/Symbols referenced:
  - INIT_MD_FILETAG
  - InvalidForkNumber
  - InvalidBlockNumber
  - [RegisterSyncRequest](../R/RegisterSyncRequest.md)
  - SYNC_FILTER_REQUEST
- Called from (representative examples):
  - [createdb_failure_callback](../c/createdb_failure_callback.md)
  - [dropdb](../d/dropdb.md)
  - [dbase_redo](../d/dbase_redo.md)
  - Referenced in MD_H header file

## Notes and Other Information
- Public function (not static), available to other modules via md.h header
- Uses InvalidForkNumber and InvalidBlockNumber as wildcards to match all files in the database
- Uses retryOnError=true to ensure filter requests are reliably processed
- Critical for efficient database drop operations
- Prevents unnecessary I/O operations during database cleanup
- Used in both normal operations (dropdb) and error recovery (createdb_failure_callback)
- Part of PostgreSQL's crash recovery system (dbase_redo)
- Significantly improves performance during database drop by bulk-canceling sync requests
- Essential for maintaining consistency during database lifecycle operations

## Simplified Source

```c
void ForgetDatabaseSyncRequests(Oid dbid) {
    FileTag tag;
    RelFileLocator rlocator;

    // Set up file locator for the database
    rlocator.dbOid = dbid;
    rlocator.spcOid = 0;
    rlocator.relNumber = 0;

    // Create filter tag to match all files in database
    INIT_MD_FILETAG(tag, rlocator, InvalidForkNumber, InvalidBlockNumber);

    // Register filter request to cancel all sync operations for this database
    RegisterSyncRequest(&tag, SYNC_FILTER_REQUEST, true);
}
```