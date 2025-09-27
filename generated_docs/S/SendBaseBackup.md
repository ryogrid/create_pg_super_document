# SendBaseBackup

## Location
[src/backend/backup/basebackup.c:988-1072](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/backup/basebackup.c#L988-L1072)

## Overview
 is the main entry point function that orchestrates a complete base backup by setting up the backup infrastructure, parsing options, and delegating to perform_base_backup.

## Definition

```c
struct stat statbuf;
```
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
  - [bbsink_cleanup](../b/bbsink_cleanup.md)
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

## Simplified Source

```c
// Simplified version of SendBaseBackup
void SendBaseBackup(BaseBackupCmd *cmd, IncrementalBackupInfo *ib) {
    basebackup_options opt;
    bbsink *sink;
    SessionBackupState status = get_backup_status();

    // Core logic step 1: Check if backup is already running
    if (status == SESSION_BACKUP_RUNNING)
        ereport(ERROR, "a backup is already in progress in this session");

    // Core logic step 2: Parse backup options from command
    parse_basebackup_options(cmd->options, &opt);

    // Core logic step 3: Set WAL sender state to backup mode
    WalSndSetState(WALSNDSTATE_BACKUP);

    // Core logic step 4: Update process title for monitoring
    if (update_process_title) {
        char activitymsg[50];
        snprintf(activitymsg, sizeof(activitymsg), "sending backup \"%s\"", opt.label);
        set_ps_display(activitymsg);
    }

    // Core logic step 5: Handle incremental vs full backup validation
    if (!opt.incremental) {
        ib = NULL;  // Ignore manifest for full backup
    } else if (ib == NULL) {
        ereport(ERROR, "must UPLOAD_MANIFEST before performing an incremental BASE_BACKUP");
    }

    // Core logic step 6: Set up backup data sink pipeline
    sink = bbsink_copystream_new(opt.send_to_client);

    // Add target-specific sink wrapper if needed
    if (opt.target_handle != NULL)
        sink = BaseBackupGetSink(opt.target_handle, sink);

    // Add throttling if requested
    if (opt.maxrate > 0)
        sink = bbsink_throttle_new(sink, opt.maxrate);

    // Add compression if requested
    if (opt.compression == PG_COMPRESSION_GZIP)
        sink = bbsink_gzip_new(sink, &opt.compression_specification);
    else if (opt.compression == PG_COMPRESSION_LZ4)
        sink = bbsink_lz4_new(sink, &opt.compression_specification);
    else if (opt.compression == PG_COMPRESSION_ZSTD)
        sink = bbsink_zstd_new(sink, &opt.compression_specification);

    // Add progress reporting
    sink = bbsink_progress_new(sink, opt.progress);

    // Core logic step 7: Perform the backup with guaranteed cleanup
    PG_TRY();
    {
        perform_base_backup(&opt, sink, ib);
    }
    PG_FINALLY();
    {
        bbsink_cleanup(sink);
    }
    PG_END_TRY();
}
```

Key simplifications made:
- Removed detailed error handling for clarity
- Consolidated sink setup into logical groupings
- Abstracted complex option parsing details
- Focused on the main execution path
- Added clear comments for each major step
- Simplified conditional logic flow