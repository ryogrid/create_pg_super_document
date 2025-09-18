# perform_rewind

## Location
src/bin/pg_rewind/pg_rewind.c: 553 - 732

## Overview
The  function executes the core rewind operation by applying all file changes identified during analysis and updating the target database's control file to ensure proper WAL replay.

## Definition


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
- : Complete file map containing all files and their required actions
- : Rewind source interface providing methods to fetch data from source system
- : LSN of the checkpoint record to use as rewind point
- : Timeline ID associated with the checkpoint
- : Redo LSN of the checkpoint (actual WAL replay start point)

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