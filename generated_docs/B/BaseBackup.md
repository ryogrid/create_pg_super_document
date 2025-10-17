# BaseBackup

## Location
[src/bin/pg_basebackup/pg_basebackup.c:1753-2354](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/pg_basebackup.c#L1753-L2354)

## Overview
The main function that orchestrates a complete PostgreSQL base backup process, handling server communication, data transfer, and backup integrity verification.

## Definition
```c
static void BaseBackup(char *compression_algorithm, char *compression_detail,
                      CompressionLocation compressloc,
                      pg_compress_specification *client_compress,
                      char *incremental_manifest)
```

## Detailed Description
BaseBackup is the central orchestration function for pg_basebackup that performs a complete PostgreSQL database backup. It handles the entire backup workflow including:

1. **Server Compatibility Checks**: Validates server version compatibility and feature support
2. **Incremental Backup Support**: Handles manifest upload for incremental backups when specified
3. **Command Building**: Constructs the BASE_BACKUP command with appropriate options based on user preferences
4. **Backup Execution**: Initiates and manages the backup process, handling both tar and plain formats
5. **WAL Streaming**: Optionally starts background WAL streaming for consistent backups
6. **Progress Reporting**: Provides backup progress information to users
7. **Data Transfer**: Manages the actual data transfer from server to client
8. **Integrity Verification**: Handles backup manifest processing and checksum verification
9. **Post-Backup Tasks**: Performs data synchronization and cleanup operations

The function supports various backup modes including plain and tar formats, server-side and client-side compression, incremental backups, and different backup targets. It also handles cross-platform differences for background process management.

## Parameters / Member Variables
- `compression_algorithm`: String specifying the compression algorithm to use (e.g., "gzip", "lz4")
- `compression_detail`: Additional compression parameters and settings 
- `compressloc`: Enumeration indicating whether compression should happen on server or client side
- `client_compress`: Compression specification structure for client-side compression settings
- `incremental_manifest`: Path to manifest file for incremental backup, or NULL for full backup

## Dependencies
- Functions called/Symbols referenced:
  - [CheckServerVersionForStreaming](../C/CheckServerVersionForStreaming.md) (server version validation)
  - [GenerateRecoveryConfig](../G/GenerateRecoveryConfig.md) (recovery configuration generation)  
  - [RunIdentifySystem](../R/RunIdentifySystem.md) (system identification)
  - [AppendStringCommandOption](../A/AppendStringCommandOption.md), AppendPlainCommandOption, AppendIntegerCommandOption (command building)
  - [StartLogStreamer](../S/StartLogStreamer.md) (WAL streaming setup)
  - [ReceiveArchiveStream](../R/ReceiveArchiveStream.md) (archive stream handling for newer servers)
  - [ReceiveTarFile](../R/ReceiveTarFile.md) (individual tar file reception for older servers)
  - [ReceiveBackupManifest](../R/ReceiveBackupManifest.md) (manifest file reception)
  - Various PQxxx functions for PostgreSQL client communication
  - [sync_dir_recurse](../s/sync_dir_recurse.md), sync_pgdata (data synchronization)
  - [durable_rename](../d/durable_rename.md) (atomic file operations)

- Called from (representative examples):
  - [main](../m/main.md) (primary entry point from command-line processing)

## Notes and Other Information
- Supports PostgreSQL servers from version 9.1 onwards with feature detection for newer capabilities
- Handles both single-tablespace and multi-tablespace database configurations
- Implements comprehensive error handling with descriptive error messages
- Supports writing output to stdout for integration with other tools
- Provides extensive logging and progress reporting capabilities  
- Uses different code paths for servers before and after version 15.0 due to protocol changes
- Includes platform-specific code for Windows vs Unix process management
- Performs atomic operations for backup manifest files to ensure consistency
- Handles backup target functionality for server-managed backup storage

## Simplified Source

```c
static void BaseBackup(char *compression_algorithm, char *compression_detail,
                      CompressionLocation compressloc,
                      pg_compress_specification *client_compress,
                      char *incremental_manifest) {
    PGresult *res;
    char *sysidentifier;
    TimeLineID latesttli, starttli;
    char xlogstart[64], xlogend[64] = {0};
    int serverVersion, serverMajor;
    bool use_new_option_syntax = false;
    PQExpBufferData buf;

    // Initialize and check server version compatibility
    initPQExpBuffer(&buf);
    serverVersion = PQserverVersion(conn);
    serverMajor = serverVersion / 100;

    if (serverMajor < 901 || serverMajor > PG_VERSION_NUM / 100)
        pg_fatal("incompatible server version");

    if (serverMajor >= 1500)
        use_new_option_syntax = true;

    // Handle incremental backup manifest upload if specified
    if (incremental_manifest != NULL) {
        // Upload manifest file to server for incremental backup
        upload_incremental_manifest(incremental_manifest);
        AppendPlainCommandOption(&buf, use_new_option_syntax, "INCREMENTAL");
    }

    // Build BASE_BACKUP command with all specified options
    build_backup_command_options(&buf, use_new_option_syntax, compression_algorithm,
                                compression_detail, compressloc);

    // Execute BASE_BACKUP command and get initial response
    execute_base_backup_command(&buf, use_new_option_syntax);

    // Get WAL start position and timeline info
    res = PQgetResult(conn);
    strlcpy(xlogstart, PQgetvalue(res, 0, 0), sizeof(xlogstart));
    starttli = (PQnfields(res) >= 2) ? atoi(PQgetvalue(res, 0, 1)) : latesttli;

    // Start WAL streaming if requested
    if (includewal == STREAM_WAL) {
        StartLogStreamer(xlogstart, starttli, sysidentifier,
                        client_compress->algorithm, client_compress->level);
    }

    // Receive backup data - different methods for different server versions
    if (serverMajor >= 1500) {
        // Newer servers: receive single archive stream
        ReceiveArchiveStream(conn, client_compress);
    } else {
        // Older servers: receive individual tar files for each tablespace
        receive_tablespace_tar_files(res, client_compress);

        // Receive backup manifest if enabled and not writing to stdout
        if (!writing_to_stdout && manifest)
            ReceiveBackupManifest(conn);
    }

    // Get backup completion info
    res = PQgetResult(conn);
    strlcpy(xlogend, PQgetvalue(res, 0, 0), sizeof(xlogend));

    // Wait for background WAL streaming to complete if active
    if (bgchild > 0)
        wait_for_wal_streaming_completion(xlogend);

    // Sync data to disk if requested and not using backup target
    if (do_sync && backup_target == NULL)
        sync_backup_data();

    // Atomically rename temporary manifest file to final name
    if (!writing_to_stdout && manifest && backup_target == NULL)
        finalize_backup_manifest();

    // Cleanup
    PQfinish(conn);
    conn = NULL;
}
```