# register_unlink_segment

## Location
[src/backend/storage/smgr/md.c:1399-1415](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/smgr/md.c#L1399-L1415)

## Overview
Schedules a file segment to be deleted after the next checkpoint by registering an unlink request with the sync request system.

## Definition
```c
static void register_unlink_segment(RelFileLocatorBackend rlocator, ForkNumber forknum, BlockNumber segno)
```

## Detailed Description
The register_unlink_segment function is responsible for scheduling the deletion of a file segment in a safe, crash-recoverable manner. Rather than immediately deleting the file, it registers an unlink request that will be processed after the next checkpoint completes.

This deferred deletion approach is crucial for maintaining crash recovery consistency. By waiting until after a checkpoint, the system ensures that any references to the file in the WAL (Write-Ahead Log) have been safely flushed and that the file deletion will not interfere with crash recovery operations.

The function creates a FileTag to identify the specific segment and uses RegisterSyncRequest with SYNC_UNLINK_REQUEST type to queue the deletion operation. The retryOnError flag is set to true, ensuring that failed unlink requests will be retried.

## Parameters / Member Variables
- `rlocator`: RelFileLocatorBackend structure containing the database, tablespace, and relation identifiers for the file to be unlinked
- `forknum`: ForkNumber indicating which fork of the relation to unlink (main, FSM, VM, etc.)
- `segno`: BlockNumber representing the segment number within the fork to be deleted

## Dependencies
- Functions called/Symbols referenced:
  - INIT_MD_FILETAG
  - RelFileLocatorBackendIsTemp
  - [RegisterSyncRequest](../R/RegisterSyncRequest.md)
  - SYNC_UNLINK_REQUEST
- Called from (representative examples):
  - [mdunlinkfork](../m/mdunlinkfork.md)

## Notes and Other Information
- Static function, only called from within the md.c file
- Includes assertion to ensure temporary relations are never processed (they are handled differently)
- Uses retryOnError=true to ensure unlink requests are not lost due to transient failures
- Part of PostgreSQL's crash-safe file management system
- Deferred deletion prevents issues with WAL replay during crash recovery
- Works in conjunction with the checkpointer process to safely remove files
- Critical for operations like DROP TABLE, TRUNCATE, and relation fork deletion