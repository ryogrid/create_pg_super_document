# SendBaseBackup

## Location
src/backend/backup/basebackup.c: 988 - 1072

## Overview
 is the main entry point function that orchestrates a complete base backup by setting up the backup infrastructure, parsing options, and delegating to perform_base_backup.

## Definition


## Detailed Description
This function serves as the high-level coordinator for base backup operations. It validates backup prerequisites, parses and applies backup options, and constructs a chain of bbsink handlers for data processing (compression, throttling, progress reporting, etc.). The function ensures proper session state management and provides robust error handling with guaranteed cleanup.

The function implements a layered architecture where multiple bbsink objects form a processing pipeline:
1. Base sink (copystream for client delivery or external target)
2. Target-specific sink wrapper (if using external target)
3. Throttling sink (if max_rate specified)  
4. Compression sink (gzip, lz4, or zstd)
5. Progress reporting sink

Key validation includes checking for concurrent backups in the same session and ensuring incremental backups have the required manifest data.

## Parameters / Member Variables
- : BaseBackupCmd structure containing parsed SQL command options and parameters
- : IncrementalBackupInfo for incremental backups, or NULL for full backups

## Dependencies
- Functions called/Symbols referenced:
  - [parse_basebackup_options](../p/parse_basebackup_options.md)
  - [get_backup_status](../g/get_backup_status.md)
  - [perform_base_backup](../p/perform_base_backup.md)
  - [WalSndSetState](../W/WalSndSetState.md)
  - [bbsink_copystream_new](../b/bbsink_copystream_new.md)
  - [BaseBackupGetSink](../B/BaseBackupGetSink.md)
  - [bbsink_throttle_new](../b/bbsink_throttle_new.md)
  - [bbsink_gzip_new](../b/bbsink_gzip_new.md)/bbsink_lz4_new/bbsink_zstd_new
  - [bbsink_progress_new](../b/bbsink_progress_new.md)
  - bbsink_cleanup
- Called from (representative examples):
  - [exec_replication_command](../e/exec_replication_command.md) (in walsender.c)

## Notes and Other Information
- Sets WAL sender state to WALSNDSTATE_BACKUP during operation
- Updates process title to show backup label for monitoring
- Uses PG_TRY/PG_FINALLY for guaranteed bbsink cleanup on errors
- Validates incremental backup requirements: manifest must be uploaded first
- Supports multiple compression algorithms through modular bbsink architecture
- Prevents concurrent backup operations within the same session
- The bbsink pipeline architecture allows flexible composition of backup processing features