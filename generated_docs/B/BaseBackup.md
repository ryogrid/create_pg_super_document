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