# perform_base_backup

## Location
[src/backend/backup/basebackup.c:234-683](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/backup/basebackup.c#L234-L683)

## Overview
 is the core function that executes the actual base backup process for specified tablespaces, handling the complete workflow from backup initialization to cleanup.

## Definition

```c
struct stat statbuf;
```
## Detailed Description
This function orchestrates the entire base backup process in PostgreSQL. It begins by calling  to initiate the backup, then systematically processes each tablespace, creating tar archives for the data. The function handles both regular and incremental backups, and optionally includes WAL files in the backup. It uses error cleanup mechanisms to ensure proper cleanup even if the backup fails partway through.

The function operates in several key phases:
1. Initialize backup state and call 
2. Calculate total backup size if progress reporting is enabled
3. Send backup_label and tablespace_map files
4. Process each tablespace, creating tar archives
5. Handle WAL file inclusion if requested
6. Finalize backup with manifest and cleanup

Key safety features include comprehensive error handling with PG_ENSURE_ERROR_CLEANUP to prevent backup counter leaks, validation of WAL file sequences, and checksum verification.

## Parameters / Member Variables
- : Configuration options controlling backup behavior (progress reporting, WAL inclusion, etc.)
- : Output destination handler for writing backup data 
- : Incremental backup information, NULL for full backups

## Dependencies
- Functions called/Symbols referenced:
  - [do_pg_backup_start](../d/do_pg_backup_start.md)
  - [do_pg_backup_stop](../d/do_pg_backup_stop.md)
  - [sendDir](../s/sendDir.md)
  - [sendTablespace](../s/sendTablespace.md)
  - [sendFileWithContent](../s/sendFileWithContent.md)
  - [build_backup_content](../b/build_backup_content.md)
  - [CheckXLogRemoved](../C/CheckXLogRemoved.md)
  - [compareWalFileNames](../c/compareWalFileNames.md)
  - [bbsink_begin_backup](../b/bbsink_begin_backup.md)/bbsink_end_backup
  - [RecoveryInProgress](../R/RecoveryInProgress.md)
- Called from (representative examples):
  - [SendBaseBackup](../S/SendBaseBackup.md)

## Notes and Other Information
- This function is static and split out primarily to avoid compiler warnings about variables potentially being clobbered by longjmp
- Uses extensive error cleanup with PG_ENSURE_ERROR_CLEANUP to ensure backup counters are properly managed
- Performs comprehensive validation of WAL file sequences when including WAL
- The main data directory is always processed last to facilitate WAL inclusion
- Supports both full and incremental backups through the IncrementalBackupInfo parameter

## Simplified Source

```c
// Simplified version of perform_base_backup
static void perform_base_backup(basebackup_options *opt, bbsink *sink,
                               IncrementalBackupInfo *ib) {
    bbsink_state state;
    XLogRecPtr endptr;
    TimeLineID endtli;
    backup_manifest_info manifest;
    BackupState *backup_state;
    StringInfo tablespace_map;

    // Initialize backup state
    initialize_backup_state(&state);
    setup_resource_owner_for_backup();

    // Initialize manifest and backup structures
    InitializeBackupManifest(&manifest, opt->manifest, opt->manifest_checksum_type);
    backup_state = (BackupState *) palloc0(sizeof(BackupState));
    tablespace_map = makeStringInfo();

    // Start the backup process
    basebackup_progress_wait_checkpoint();
    do_pg_backup_start(opt->label, opt->fastcheckpoint, &state.tablespaces,
                      backup_state, tablespace_map);

    state.startptr = backup_state->startpoint;
    state.starttli = backup_state->starttli;

    // Ensure proper cleanup on any error
    PG_ENSURE_ERROR_CLEANUP(do_pg_abort_backup, BoolGetDatum(false));
    {
        // Prepare for incremental backup if needed
        if (ib != NULL)
            PrepareForIncrementalBackup(ib, backup_state);

        // Calculate backup size for progress reporting
        if (opt->progress) {
            calculate_total_backup_size(&state);
        }

        // Begin backup processing
        bbsink_begin_backup(sink, &state, SINK_BUFFER_LENGTH);

        // Process each tablespace
        foreach(lc, state.tablespaces) {
            tablespaceinfo *ti = (tablespaceinfo *) lfirst(lc);

            if (ti->path == NULL) {
                // Process main data directory
                process_main_data_directory(sink, &state, opt, backup_state,
                                           tablespace_map, &manifest, ib);
            } else {
                // Process individual tablespace
                process_tablespace(sink, ti, &manifest, ib);
            }

            // Handle WAL inclusion or terminate archive
            if (should_include_wal_after_tablespace(opt, ti))
                break; // WAL will be processed separately
            else
                terminate_current_archive(sink);
        }

        // Complete backup process
        basebackup_progress_wait_wal_archive(&state);
        do_pg_backup_stop(backup_state, !opt->nowait);

        endptr = backup_state->stoppoint;
        endtli = backup_state->stoptli;

        // Cleanup backup structures
        cleanup_backup_structures(tablespace_map, backup_state);
    }
    PG_END_ENSURE_ERROR_CLEANUP(do_pg_abort_backup, BoolGetDatum(false));

    // Include WAL files if requested
    if (opt->includewal) {
        include_wal_files_in_backup(sink, &state, endptr, endtli, &manifest);
    }

    // Finalize backup with manifest and cleanup
    finalize_backup_process(sink, &manifest, &state, endptr, endtli);
}

// Helper functions for clarity
static void initialize_backup_state(bbsink_state *state) {
    state->tablespaces = NIL;
    state->tablespace_num = 0;
    state->bytes_done = 0;
    state->bytes_total = 0;
    state->bytes_total_is_valid = false;
}

static void process_main_data_directory(bbsink *sink, bbsink_state *state,
                                       basebackup_options *opt, BackupState *backup_state,
                                       StringInfo tablespace_map, backup_manifest_info *manifest,
                                       IncrementalBackupInfo *ib) {
    // Begin main data archive
    bbsink_begin_archive(sink, "base.tar");

    // Send backup_label file first
    char *backup_label = build_backup_content(backup_state, false);
    sendFileWithContent(sink, BACKUP_LABEL_FILE, backup_label, -1, manifest);

    // Send tablespace_map if requested
    if (opt->sendtblspcmapfile) {
        sendFileWithContent(sink, TABLESPACE_MAP, tablespace_map->data, -1, manifest);
    }

    // Send main directory contents
    sendDir(sink, ".", 1, false, state->tablespaces, !opt->sendtblspcmapfile,
            manifest, InvalidOid, ib);

    // Send pg_control last
    send_pg_control_file(sink, manifest);
}

static void include_wal_files_in_backup(bbsink *sink, bbsink_state *state,
                                       XLogRecPtr endptr, TimeLineID endtli,
                                       backup_manifest_info *manifest) {
    // Determine WAL file range
    XLogSegNo startsegno, endsegno;
    calculate_wal_file_range(state->startptr, endptr, &startsegno, &endsegno);

    // Collect WAL and timeline history files
    List *walFileList = collect_wal_files(startsegno, endsegno);
    List *historyFileList = collect_timeline_history_files();

    // Validate WAL file sequence
    validate_wal_file_sequence(walFileList, startsegno, endsegno);

    // Send WAL files
    send_wal_files(sink, walFileList);
    send_timeline_history_files(sink, historyFileList, manifest);

    // Terminate the archive
    terminate_current_archive(sink);
}
```

Key simplifications made:
- Broke down the massive function into logical helper functions
- Separated main data directory processing from tablespace processing
- Extracted WAL file inclusion into a dedicated function
- Simplified error handling while preserving PG_ENSURE_ERROR_CLEANUP pattern
- Abstracted complex file collection and validation logic
- Maintained all essential backup workflow steps
- Preserved progress reporting and manifest generation
- Kept all safety checks and validation intact