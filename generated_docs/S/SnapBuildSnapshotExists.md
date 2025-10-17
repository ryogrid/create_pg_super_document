# SnapBuildSnapshotExists

## Location
[src/backend/replication/logical/snapbuild.c:2206-2223](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/snapbuild.c#L2206-L2223)

## Overview
SnapBuildSnapshotExists checks whether a serialized logical snapshot exists at a specified Write-Ahead Log (WAL) position, used in PostgreSQL's logical replication infrastructure.

## Definition

```c
struct stat stat_buf;
```
## Detailed Description
This function determines if a logical snapshot has been previously serialized to disk at the given Log Sequence Number (LSN). It constructs the expected file path for the snapshot file based on the LSN and uses the  system call to check for its existence. The function is part of PostgreSQL's snapshot building mechanism for logical replication, which allows logical decoding processes to restore consistent snapshots from persistent storage.

The snapshot files are stored in the  directory with a naming convention based on the LSN format: . This naming scheme ensures unique identification of snapshots at specific WAL positions.

## Parameters / Member Variables
- : XLogRecPtr representing the Write-Ahead Log position where the snapshot should exist. This is used to construct the snapshot filename and locate the corresponding serialized snapshot file.

## Dependencies
- Functions called/Symbols referenced:
  -  (standard C library function for string formatting)
  -  (system call to get file status information)
  -  (PostgreSQL macro for formatting LSN values)
  -  (PostgreSQL error reporting function)
  -  (PostgreSQL error code function)
  -  (PostgreSQL error message function)
- Called from (representative examples):
  -  (in src/backend/replication/logical/slotsync.c:244)

## Notes and Other Information
- The function returns  if the snapshot file exists,  otherwise
- File path construction uses MAXPGPATH to ensure buffer safety
- Error handling distinguishes between file non-existence (ENOENT) and other stat() failures
- The function is located in src/backend/replication/logical/snapbuild.c:2206-2223
- This is primarily used in logical replication slot synchronization to verify snapshot availability before attempting to restore logical decoding state
- The snapshot files contain serialized transaction visibility information necessary for consistent logical decoding

## Simplified Source

```c
bool
SnapBuildSnapshotExists(XLogRecPtr lsn)
{
    char path[MAXPGPATH];
    int ret;
    struct stat stat_buf;

    // Construct snapshot filename based on LSN
    sprintf(path, "pg_logical/snapshots/%X-%X.snap",
            LSN_FORMAT_ARGS(lsn));

    // Check if file exists using stat()
    ret = stat(path, &stat_buf);

    // Handle errors other than file not found
    if (ret != 0 && errno != ENOENT)
        ereport(ERROR, (errcode_for_file_access(),
                       errmsg("could not stat file \"%s\": %m", path)));

    return ret == 0;
}
```