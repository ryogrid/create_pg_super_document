# basebackup_options

## Location
[src/backend/backup/basebackup.c:79-136](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/backup/basebackup.c#L79-L136)

## Overview
The `basebackup_options` structure encapsulates configuration parameters and settings for PostgreSQL base backup operations, controlling various aspects of backup behavior including compression, progress reporting, and manifest generation.

## Definition
```c
typedef struct
{
    const char *label;
    bool        progress;
    bool        fastcheckpoint;
    bool        nowait;
    bool        includewal;
    bool        incremental;
    uint32      maxrate;
    bool        sendtblspcmapfile;
    bool        send_to_client;
    bool        use_copytblspc;
    BaseBackupTargetHandle *target_handle;
    backup_manifest_option manifest;
    pg_compress_algorithm compression;
    pg_compress_specification compression_specification;
    pg_checksum_type manifest_checksum_type;
} basebackup_options;
```

## Detailed Description
The `basebackup_options` structure serves as a comprehensive configuration container for base backup operations in PostgreSQL. It consolidates all the various options and settings that can be specified when performing a base backup, ranging from basic behavioral flags to advanced compression and manifest generation settings. This structure is used throughout the base backup subsystem to pass configuration parameters between functions and ensure consistent behavior across the backup process.

## Parameters / Member Variables
- `label`: A string identifier/label for the backup operation
- `progress`: Boolean flag indicating whether to report progress during backup
- `fastcheckpoint`: Boolean flag to request a fast checkpoint before backup starts
- `nowait`: Boolean flag indicating whether to wait for checkpoint completion or fail immediately if checkpoint is already in progress
- `includewal`: Boolean flag to include WAL (Write-Ahead Log) files in the backup
- `incremental`: Boolean flag to enable incremental backup mode
- `maxrate`: Maximum transfer rate limit in KB/s (0 means unlimited)
- `sendtblspcmapfile`: Boolean flag to include tablespace mapping file in backup
- `send_to_client`: Boolean flag indicating whether backup data should be sent to client
- `use_copytblspc`: Boolean flag to control tablespace copying behavior
- `target_handle`: Handle for backup target destination management
- `manifest`: Enumeration controlling backup manifest generation options
- `compression`: Algorithm to use for backup compression
- `compression_specification`: Detailed compression parameters and settings
- `manifest_checksum_type`: Checksum algorithm to use for manifest validation

## Dependencies
- Functions called/Symbols referenced:
  - [BaseBackupTargetHandle](../B/BaseBackupTargetHandle.md)
  - [backup_manifest_option](backup_manifest_option.md)
  - [pg_compress_algorithm](../p/pg_compress_algorithm.md)
  - [pg_compress_specification](../p/pg_compress_specification.md)
  - pg_checksum_type
- Called from (representative examples):
  - [perform_base_backup](../p/perform_base_backup.md)
  - [parse_basebackup_options](../p/parse_basebackup_options.md)
  - [SendBaseBackup](../S/SendBaseBackup.md)

## Notes and Other Information
This structure is defined in src/backend/backup/basebackup.c:62-79 and serves as the primary configuration interface for PostgreSQL base backup operations. The structure is typically populated by parsing command-line options or protocol messages and then passed to the core backup execution functions. The design allows for extensible configuration management while maintaining a clean interface between option parsing and backup execution logic.