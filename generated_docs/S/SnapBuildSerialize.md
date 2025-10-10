# SnapBuildSerialize

## Location
[src/backend/replication/logical/snapbuild.c:1669-1908](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/snapbuild.c#L1669-L1908)

## Overview
SnapBuildSerialize writes a snapshot builder's current state to disk as a serialized file at a specific LSN location, enabling recovery and continuation of logical replication decoding processes.

## Definition
```c
static void SnapBuildSerialize(SnapBuild *builder, XLogRecPtr lsn)
```

## Detailed Description
This function serializes the complete state of a snapshot builder to a disk file, allowing logical replication to resume from a specific point without rebuilding the entire snapshot from the beginning. The serialization process:

1. **State Validation**: Only serializes if the builder is in SNAPBUILD_CONSISTENT state or later
2. **File Management**: Creates snapshot files with LSN-based naming (pg_logical/snapshots/X-X.snap)
3. **Concurrency Safety**: Handles race conditions where multiple backends might serialize the same LSN
4. **Data Packaging**: Combines SnapBuild structure with committed transaction lists and catalog-changing transactions
5. **Integrity**: Uses CRC32C checksums to ensure data integrity
6. **Atomic Operations**: Uses temporary files and atomic rename to ensure consistency

The function creates a binary file containing the SnapBuildOnDisk structure, which includes the main SnapBuild state plus arrays of transaction IDs for committed and catalog-changing transactions.

## Parameters / Member Variables
- `builder`: Pointer to the SnapBuild structure containing the current snapshot state to serialize
- `lsn`: XLog record pointer indicating the WAL position where this snapshot is valid

## Dependencies
- Functions called/Symbols referenced:
  - [ReorderBufferGetCatalogChangesXacts](../R/ReorderBufferGetCatalogChangesXacts.md)
  - [dclist_count](../d/dclist_count.md)
  - [OpenTransientFile](../O/OpenTransientFile.md)
  - write/pg_fsync/CloseTransientFile
  - [fsync_fname](../f/fsync_fname.md)
  - rename
  - [ReorderBufferSetRestartPoint](../R/ReorderBufferSetRestartPoint.md)
  - SNAPBUILD_MAGIC/SNAPBUILD_VERSION (constants)
  - CRC32C checksum functions (INIT_CRC32C, COMP_CRC32C, FIN_CRC32C)
- Called from (representative examples):
  - [SnapBuildProcessRunningXacts](SnapBuildProcessRunningXacts.md)
  - [SnapBuildSerializationPoint](SnapBuildSerializationPoint.md)

## Notes and Other Information
- Only serializes snapshots that have reached SNAPBUILD_CONSISTENT state to ensure they are usable for decoding
- File naming scheme uses LSN format: pg_logical/snapshots/[LSN].snap
- Handles concurrent serialization attempts gracefully - if another process has already serialized the same LSN, it skips the work
- Uses temporary files with process ID suffix to avoid conflicts during concurrent operations
- Performs multiple fsync operations to ensure durability: on the file itself, and on the directory
- Memory allocation uses the builder's memory context
- Includes both committed transactions and catalog-changing transactions in the serialized data
- The serialized format includes magic numbers, version information, and CRC32C checksums for validation
- Sets a restart point in the reorder buffer after successful serialization

## Simplified Source

```c
static void SnapBuildSerialize(SnapBuild *builder, XLogRecPtr lsn)
{
    SnapBuildOnDisk *ondisk = NULL;
    TransactionId *catchange_xip = NULL;
    char path[MAXPGPATH];
    char tmppath[MAXPGPATH];
    int fd;
    Size needed_length;
    struct stat stat_buf;

    // Only serialize if we have a consistent snapshot
    if (builder->state < SNAPBUILD_CONSISTENT)
        return;

    // Create snapshot file path based on LSN
    sprintf(path, "pg_logical/snapshots/%X-%X.snap", LSN_FORMAT_ARGS(lsn));

    // Check if another backend already serialized this LSN
    if (stat(path, &stat_buf) == 0) {
        fsync_fname(path, false);
        fsync_fname("pg_logical/snapshots", true);
        builder->last_serialized_snapshot = lsn;
        goto out;
    }

    // Create temporary file for atomic write
    sprintf(tmppath, "pg_logical/snapshots/%X-%X.snap.%d.tmp",
            LSN_FORMAT_ARGS(lsn), MyProcPid);

    // Get catalog modifying transactions
    catchange_xip = ReorderBufferGetCatalogChangesXacts(builder->reorder);
    size_t catchange_xcnt = dclist_count(&builder->reorder->catchange_txns);

    // Calculate total size needed
    needed_length = sizeof(SnapBuildOnDisk) +
                   sizeof(TransactionId) * (builder->committed.xcnt + catchange_xcnt);

    // Prepare serialization structure
    ondisk = palloc0(needed_length);
    ondisk->magic = SNAPBUILD_MAGIC;
    ondisk->version = SNAPBUILD_VERSION;
    ondisk->length = needed_length;

    // Copy builder state and clear memory-only pointers
    memcpy(&ondisk->builder, builder, sizeof(SnapBuild));
    ondisk->builder.context = NULL;
    ondisk->builder.snapshot = NULL;
    ondisk->builder.reorder = NULL;
    ondisk->builder.committed.xip = NULL;
    ondisk->builder.catchange.xip = NULL;

    // Copy transaction arrays after main structure
    char *ondisk_c = (char *)ondisk + sizeof(SnapBuildOnDisk);
    if (builder->committed.xcnt > 0) {
        memcpy(ondisk_c, builder->committed.xip,
               sizeof(TransactionId) * builder->committed.xcnt);
        ondisk_c += sizeof(TransactionId) * builder->committed.xcnt;
    }
    if (catchange_xcnt > 0) {
        memcpy(ondisk_c, catchange_xip, sizeof(TransactionId) * catchange_xcnt);
    }

    // Write to temporary file atomically
    fd = OpenTransientFile(tmppath, O_CREAT | O_EXCL | O_WRONLY | PG_BINARY);
    write(fd, ondisk, needed_length);
    pg_fsync(fd);
    CloseTransientFile(fd);

    // Atomic rename to final location
    rename(tmppath, path);
    fsync_fname(path, false);
    fsync_fname("pg_logical/snapshots", true);

    builder->last_serialized_snapshot = lsn;

out:
    ReorderBufferSetRestartPoint(builder->reorder, builder->last_serialized_snapshot);
    if (ondisk) pfree(ondisk);
    if (catchange_xip) pfree(catchange_xip);
}
```