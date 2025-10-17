# ReceiveTarFile

## Location
[src/bin/pg_basebackup/pg_basebackup.c:1599-1661](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/pg_basebackup.c#L1599-L1661)

## Overview
ReceiveTarFile is the main function in pg_basebackup responsible for receiving raw tar data from the PostgreSQL server during a base backup operation and streaming it to the appropriate destination with optional manifest injection.

## Definition
```c
static void ReceiveTarFile(PGconn *conn, char *archive_name, char *spclocation,
                          bool tablespacenum, pg_compress_specification *compress)
```

## Detailed Description
This function orchestrates the process of receiving a tar file from the PostgreSQL server during base backup operations. It sets up a backup streaming pipeline that handles data compression, manifest injection, and output formatting based on server version capabilities and user preferences.

The function first determines server capabilities by checking version numbers for recovery GUC support and terminated tarfile support. It then creates an appropriate backup streamer using CreateBackupStreamer, which handles the complexities of output formatting (tar to stdout vs individual files) and compression.

The core data transfer is handled by ReceiveCopyData with ReceiveTarCopyChunk as the callback function. After the main data transfer, the function conditionally handles backup manifest injection if required (when writing to stdout as a single tarfile). Finally, it performs cleanup operations including streamer finalization and progress reporting.

The function is designed to handle version compatibility issues, ensuring proper operation across different PostgreSQL server versions with varying feature support.

## Parameters / Member Variables
- `conn`: Active PostgreSQL connection handle for receiving data from the server
- `archive_name`: Name of the archive/tablespace being received (used for output file naming)
- `spclocation`: Location path for the tablespace (NULL for main data directory)
- `tablespacenum`: Boolean flag indicating whether this is a numbered tablespace (affects progress reporting)
- `compress`: Compression specification structure containing compression algorithm and parameters

## Dependencies
- Functions called/Symbols referenced:
  - [CreateBackupStreamer](../C/CreateBackupStreamer.md)
  - [ReceiveCopyData](ReceiveCopyData.md)
  - [ReceiveTarCopyChunk](ReceiveTarCopyChunk.md)
  - [ReceiveBackupManifestInMemory](ReceiveBackupManifestInMemory.md)
  - [bbstreamer_inject_file](../b/bbstreamer_inject_file.md)
  - [bbstreamer_finalize](../b/bbstreamer_finalize.md)
  - [bbstreamer_free](../b/bbstreamer_free.md)
  - [progress_update_filename](../p/progress_update_filename.md)
  - [progress_report](../p/progress_report.md)
  - [PQserverVersion](../P/PQserverVersion.md)
- Called from (representative examples):
  - [BaseBackup](../B/BaseBackup.md)

## Notes and Other Information
- This is a static function, only accessible within the pg_basebackup.c compilation unit
- Server version checks ensure compatibility with different PostgreSQL versions
- Backup manifest injection is conditional and only occurs when outputting to a single tarfile to stdout
- The function handles both main data directory and tablespace tar files
- Progress reporting is integrated to provide user feedback during long-running operations
- File synchronization is deferred - all files are synced together at the end of the backup process
- The function uses the bbstreamer API for flexible output formatting and compression handling

## Simplified Source

```c
static void
ReceiveTarFile(PGconn *conn, char *archive_name, char *spclocation,
               bool tablespacenum, pg_compress_specification *compress)
{
    WriteTarState state;
    bbstreamer *manifest_inject_streamer;
    bool is_recovery_guc_supported;
    bool expect_unterminated_tarfile;

    // Initialize state and check server capabilities
    memset(&state, 0, sizeof(state));
    is_recovery_guc_supported =
        PQserverVersion(conn) >= MINIMUM_VERSION_FOR_RECOVERY_GUC;
    expect_unterminated_tarfile =
        PQserverVersion(conn) < MINIMUM_VERSION_FOR_TERMINATED_TARFILE;

    // Create backup streamer for handling output
    state.streamer = CreateBackupStreamer(archive_name, spclocation,
                                         &manifest_inject_streamer,
                                         is_recovery_guc_supported,
                                         expect_unterminated_tarfile,
                                         compress);
    state.tablespacenum = tablespacenum;

    // Receive and process tar data
    ReceiveCopyData(conn, ReceiveTarCopyChunk, &state);
    progress_update_filename(NULL);

    // Handle manifest injection if needed
    if (manifest_inject_streamer != NULL) {
        PQExpBufferData buf;

        // Load backup manifest into memory
        initPQExpBuffer(&buf);
        ReceiveBackupManifestInMemory(conn, &buf);
        if (PQExpBufferDataBroken(buf))
            pg_fatal("out of memory");

        // Inject manifest into tarfile
        bbstreamer_inject_file(manifest_inject_streamer, "backup_manifest",
                              buf.data, buf.len);

        termPQExpBuffer(&buf);
    }

    // Cleanup and finalize
    bbstreamer_finalize(state.streamer);
    bbstreamer_free(state.streamer);
    progress_report(tablespacenum, true, false);
}
```