# SnapBuildRestore

## Location
[src/backend/replication/logical/snapbuild.c:1909-2080](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/snapbuild.c#L1909-L2080)

## Overview
SnapBuildRestore loads and validates a previously serialized snapshot from disk, restoring the snapshot builder's state to enable continuation of logical replication decoding.

## Definition
```c
static bool SnapBuildRestore(SnapBuild *builder, XLogRecPtr lsn)
```

## Detailed Description
This function attempts to restore a snapshot builder from a serialized file on disk, allowing logical replication to resume from a previous consistent point. The restoration process involves:

1. **File Location**: Constructs the snapshot file path using the LSN (pg_logical/snapshots/X-X.snap)
2. **File Validation**: Verifies file existence and accessibility
3. **Data Integrity**: Performs comprehensive validation including magic numbers, version compatibility, and CRC32C checksum verification
4. **Content Restoration**: Reads and reconstructs the SnapBuild structure, committed transaction arrays, and catalog-changing transaction arrays
5. **State Assessment**: Evaluates whether the restored snapshot is more advanced and usable than the current state
6. **State Transfer**: If suitable, copies the restored state to the current builder and rebuilds the snapshot

The function returns true if a usable snapshot was successfully restored, false otherwise.

## Parameters / Member Variables
- `builder`: Pointer to the SnapBuild structure to restore state into
- `lsn`: XLog record pointer indicating the WAL position where the snapshot was serialized

## Dependencies
- Functions called/Symbols referenced:
  - [SnapBuildRestoreContents](SnapBuildRestoreContents.md)
  - [OpenTransientFile](../O/OpenTransientFile.md)/CloseTransientFile
  - [fsync_fname](../f/fsync_fname.md)
  - [MemoryContextAllocZero](../M/MemoryContextAllocZero.md)
  - CRC32C checksum functions (INIT_CRC32C, COMP_CRC32C, FIN_CRC32C, EQ_CRC32C)
  - [TransactionIdPrecedes](../T/TransactionIdPrecedes.md)
  - [SnapBuildSnapDecRefcount](SnapBuildSnapDecRefcount.md)/SnapBuildSnapIncRefcount
  - [SnapBuildBuildSnapshot](SnapBuildBuildSnapshot.md)
  - [ReorderBufferSetRestartPoint](../R/ReorderBufferSetRestartPoint.md)
  - SNAPBUILD_MAGIC/SNAPBUILD_VERSION (constants)
- Called from (representative examples):
  - [SnapBuildFindSnapshot](SnapBuildFindSnapshot.md)
  - [SnapBuildSerializationPoint](SnapBuildSerializationPoint.md)

## Notes and Other Information
- Only attempts restoration if current builder state is not already SNAPBUILD_CONSISTENT
- Performs extensive validation to ensure data integrity: magic numbers, version compatibility, and CRC32C checksums
- Rejects snapshots that are not in SNAPBUILD_CONSISTENT state (incomplete snapshots)
- Rejects snapshots whose xmin precedes the builder's initial_xmin_horizon (cannot guarantee transaction visibility)
- Uses the builder's memory context for allocating restored transaction arrays
- After successful restoration, rebuilds the actual snapshot structure and increments its reference count
- Sets a restart point in the reorder buffer to the restored LSN
- Logs successful restoration with details about the consistent point found
- Handles memory cleanup for rejected snapshots to prevent leaks
- File operations include explicit fsync calls to ensure data durability
- Returns false for non-existent files (normal case) but errors on other file access problems

## Simplified Source

```c
static bool SnapBuildRestore(SnapBuild *builder, XLogRecPtr lsn)
{
    SnapBuildOnDisk ondisk;
    char path[MAXPGPATH];
    int fd;
    Size sz;
    pg_crc32c checksum;

    // Skip if already consistent
    if (builder->state == SNAPBUILD_CONSISTENT)
        return false;

    // Construct snapshot file path
    sprintf(path, "pg_logical/snapshots/%X-%X.snap", LSN_FORMAT_ARGS(lsn));

    // Try to open the snapshot file
    fd = OpenTransientFile(path, O_RDONLY | PG_BINARY);
    if (fd < 0) {
        if (errno == ENOENT)
            return false;  // File doesn't exist, normal case
        else
            ereport(ERROR, (errmsg("could not open file \"%s\": %m", path)));
    }

    // Ensure file is synced to disk
    fsync_fname(path, false);
    fsync_fname("pg_logical/snapshots", true);

    // Read and validate the snapshot header
    SnapBuildRestoreContents(fd, (char *) &ondisk, SnapBuildOnDiskConstantSize, path);

    // Validate magic number and version
    if (ondisk.magic != SNAPBUILD_MAGIC)
        ereport(ERROR, (errmsg("snapshot file has wrong magic number")));
    if (ondisk.version != SNAPBUILD_VERSION)
        ereport(ERROR, (errmsg("snapshot file has unsupported version")));

    // Read main builder structure
    SnapBuildRestoreContents(fd, (char *) &ondisk.builder, sizeof(SnapBuild), path);

    // Restore committed transaction arrays
    if (ondisk.builder.committed.xcnt > 0) {
        sz = sizeof(TransactionId) * ondisk.builder.committed.xcnt;
        ondisk.builder.committed.xip = MemoryContextAllocZero(builder->context, sz);
        SnapBuildRestoreContents(fd, (char *) ondisk.builder.committed.xip, sz, path);
    }

    // Restore catalog-changing transaction arrays
    if (ondisk.builder.catchange.xcnt > 0) {
        sz = sizeof(TransactionId) * ondisk.builder.catchange.xcnt;
        ondisk.builder.catchange.xip = MemoryContextAllocZero(builder->context, sz);
        SnapBuildRestoreContents(fd, (char *) ondisk.builder.catchange.xip, sz, path);
    }

    CloseTransientFile(fd);

    // Validate restored snapshot usability
    if (ondisk.builder.state < SNAPBUILD_CONSISTENT)
        goto snapshot_not_useful;
    if (TransactionIdPrecedes(ondisk.builder.xmin, builder->initial_xmin_horizon))
        goto snapshot_not_useful;

    // Copy restored state to current builder
    builder->xmin = ondisk.builder.xmin;
    builder->xmax = ondisk.builder.xmax;
    builder->state = ondisk.builder.state;
    builder->next_phase_at = InvalidTransactionId;

    // Transfer committed transaction info
    if (builder->committed.xcnt > 0 && ondisk.builder.committed.xcnt > 0) {
        pfree(builder->committed.xip);
        builder->committed.xcnt_space = ondisk.builder.committed.xcnt;
        builder->committed.xip = ondisk.builder.committed.xip;
        ondisk.builder.committed.xip = NULL;
    }

    // Transfer catalog change info
    if (builder->catchange.xip)
        pfree(builder->catchange.xip);
    builder->catchange.xcnt = ondisk.builder.catchange.xcnt;
    builder->catchange.xip = ondisk.builder.catchange.xip;
    ondisk.builder.catchange.xip = NULL;

    // Rebuild snapshot and set restart point
    if (builder->snapshot != NULL)
        SnapBuildSnapDecRefcount(builder->snapshot);
    builder->snapshot = SnapBuildBuildSnapshot(builder);
    SnapBuildSnapIncRefcount(builder->snapshot);

    ReorderBufferSetRestartPoint(builder->reorder, lsn);

    ereport(LOG, (errmsg("logical decoding found consistent point at %X/%X",
                        LSN_FORMAT_ARGS(lsn))));
    return true;

snapshot_not_useful:
    // Clean up unused restored data
    if (ondisk.builder.committed.xip != NULL)
        pfree(ondisk.builder.committed.xip);
    if (ondisk.builder.catchange.xip != NULL)
        pfree(ondisk.builder.catchange.xip);
    return false;
}
```