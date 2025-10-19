# perform_rewind

## Location
[src/bin/pg_rewind/pg_rewind.c:553-732](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_rewind/pg_rewind.c#L553-L732)

## Overview
The  function executes the core rewind operation by applying all file changes identified during analysis and updating the target database's control file to ensure proper WAL replay.

## Definition

```c
static void
perform_rewind(filemap_t *filemap, rewind_source *source,
			   XLogRecPtr chkptrec,
			   TimeLineID chkpttli,
			   XLogRecPtr chkptredo)
```
## Detailed Description
The  function is the core execution engine of the pg_rewind utility. After all analysis and planning is complete, this function carries out the actual file system modifications needed to rewind the target database to the specified point in time.

The function operates in several key phases:

1. **File map execution**: Iterates through all entries in the file map and executes the appropriate action for each file (copy, truncate, remove, create, etc.)
2. **Page-level modifications**: For relation files, copies specific modified data pages from the source to target
3. **Range fetching**: Handles partial file updates by fetching specific byte ranges
4. **Control file update**: Fetches the latest control file from source and updates the target's control file with appropriate recovery parameters
5. **Backup label creation**: Creates a backup label file to direct WAL replay start point
6. **Recovery point calculation**: Determines the correct minRecoveryPoint based on source server state (production vs standby)

The function handles different source types (local directory vs live server connection) and ensures data consistency throughout the process.

## Parameters / Member Variables
- `*filemap`: Complete file map containing all files and their required actions
- `*source`: Rewind source interface providing methods to fetch data from source system
- `chkptrec`: LSN of the checkpoint record to use as rewind point
- `chkpttli`: Timeline ID associated with the checkpoint
- `chkptredo`: Redo LSN of the checkpoint (actual WAL replay start point)
## Dependencies
- Functions called/Symbols referenced:
  -  (iterate over modified data pages)
  -  (get next modified block number)
  -  (truncate files to correct size)
  -  (remove files from target)
  -  (create new files/directories)
  -  (close any open target files)
  -  (update progress display)
  -  (parse control file contents)
  -  (create backup label for recovery)
  -  (write new control file)
  -  (memory deallocation)
  -  (logging)
  -  (error reporting and exit)

- Called from (representative examples):
  -  at src/bin/pg_rewind/pg_rewind.c:522

## Notes and Other Information
- This is a static function only accessible within pg_rewind.c
- The function includes sanity checks to detect if the source system was modified during the rewind operation
- Handles both local directory sources and live PostgreSQL server connections differently
- The control file update is critical - it sets the database state to DB_IN_ARCHIVE_RECOVERY and configures minRecoveryPoint
- For standby sources, uses minRecoveryPoint; for production sources, uses current WAL insert location
- The backup label creation ensures WAL replay starts from the correct checkpoint redo point
- Progress reporting is integrated throughout the operation
- Error handling includes detailed messages for debugging rewind failures
- Respects the dry_run mode by skipping actual control file updates when enabled
- Located at src/bin/pg_rewind/pg_rewind.c:553-732

## Simplified Source

```c
static void perform_rewind(filemap_t *filemap, rewind_source *source,
                          XLogRecPtr chkptrec, TimeLineID chkpttli, XLogRecPtr chkptredo)
{
    XLogRecPtr endrec;
    TimeLineID endtli;
    ControlFileData ControlFile_new;

    // Execute all file actions from the analysis phase
    for (int i = 0; i < filemap->nentries; i++) {
        file_entry_t *entry = filemap->entries[i];

        // Copy modified data pages for relation files
        if (entry->target_pages_to_overwrite.bitmapsize > 0) {
            datapagemap_iterator_t *iter = datapagemap_iterate(&entry->target_pages_to_overwrite);
            BlockNumber blkno;
            while (datapagemap_next(iter, &blkno)) {
                off_t offset = blkno * BLCKSZ;
                source->queue_fetch_range(source, entry->path, offset, BLCKSZ);
            }
            pg_free(iter);
        }

        // Execute the main file action
        switch (entry->action) {
            case FILE_ACTION_COPY:
                source->queue_fetch_file(source, entry->path, entry->source_size);
                break;
            case FILE_ACTION_TRUNCATE:
                truncate_target_file(entry->path, entry->source_size);
                break;
            case FILE_ACTION_COPY_TAIL:
                source->queue_fetch_range(source, entry->path, entry->target_size,
                                        entry->source_size - entry->target_size);
                break;
            case FILE_ACTION_REMOVE:
                remove_target(entry);
                break;
            case FILE_ACTION_CREATE:
                create_target(entry);
                break;
            case FILE_ACTION_NONE:
                // No action needed
                break;
        }
    }

    // Complete all queued data transfers
    source->finish_fetch(source);
    close_target_file();
    progress_report(true);

    // Fetch and validate the source control file
    size_t size;
    char *buffer = source->fetch_file(source, "global/pg_control", &size);
    digestControlFile(&ControlFile_source_after, buffer, size);
    pg_free(buffer);

    // Sanity check for local sources
    if (datadir_source &&
        memcmp(&ControlFile_source, &ControlFile_source_after, sizeof(ControlFileData)) != 0) {
        pg_fatal("source system was modified while pg_rewind was running");
    }

    // Adjust checkpoint info if source has newer restartpoint
    if (ControlFile_source.checkPointCopy.redo < chkptredo) {
        chkptredo = ControlFile_source.checkPointCopy.redo;
        chkpttli = ControlFile_source.checkPointCopy.ThisTimeLineID;
        chkptrec = ControlFile_source.checkPoint;
    }

    // Create backup label for WAL replay
    createBackupLabel(chkptredo, chkpttli, chkptrec);

    // Determine recovery endpoint based on source type
    if (connstr_source) {
        // Live server source
        if (ControlFile_source_after.state == DB_IN_ARCHIVE_RECOVERY) {
            // Standby server - use minRecoveryPoint
            endrec = ControlFile_source_after.minRecoveryPoint;
            endtli = ControlFile_source_after.minRecoveryPointTLI;
        } else {
            // Production server - use current WAL insert location
            endrec = source->get_current_wal_insert_lsn(source);
            endtli = Max(ControlFile_source_after.checkPointCopy.ThisTimeLineID,
                        ControlFile_source_after.minRecoveryPointTLI);
        }
    } else {
        // Local directory source - use shutdown checkpoint
        endrec = ControlFile_source_after.checkPoint;
        endtli = ControlFile_source_after.checkPointCopy.ThisTimeLineID;
    }

    // Update target control file for recovery
    memcpy(&ControlFile_new, &ControlFile_source_after, sizeof(ControlFileData));
    ControlFile_new.minRecoveryPoint = endrec;
    ControlFile_new.minRecoveryPointTLI = endtli;
    ControlFile_new.state = DB_IN_ARCHIVE_RECOVERY;

    if (!dry_run)
        update_controlfile(datadir_target, &ControlFile_new, do_sync);
}
```