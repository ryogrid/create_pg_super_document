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
  - OpenTransientFile
  - write/pg_fsync/CloseTransientFile
  - [fsync_fname](../f/fsync_fname.md)
  - rename
  - ReorderBufferSetRestartPoint
  - SNAPBUILD_MAGIC/SNAPBUILD_VERSION (constants)
  - CRC32C checksum functions (INIT_CRC32C, COMP_CRC32C, FIN_CRC32C)
- Called from (representative examples):
  - SnapBuildProcessRunningXacts
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