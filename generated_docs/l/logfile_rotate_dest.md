# logfile_rotate_dest

## Location
[src/backend/postmaster/syslogger.c:1263-1361](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/syslogger.c#L1263-L1361)

## Overview
Performs log file rotation for a single destination type (stderr, CSV, or JSON), handling file closure, creation, and metadata updates.

## Definition
```c
static bool logfile_rotate_dest(bool time_based_rotation, int size_rotation_for,
                               pg_time_t fntime, int target_dest,
                               char **last_file_name, FILE **logFile)
```

## Detailed Description
This function manages the rotation process for individual log file destinations within PostgreSQL's logging system. It handles the complete rotation workflow including closing old files, generating new filenames, opening new files, and updating tracking information.

The function first checks if the target destination has been disabled and closes/cleans up resources if necessary. It then determines whether rotation is needed based on time or size constraints. The function intelligently decides between truncating (overwriting) or appending to log files based on Log_truncate_on_rotation setting, time-based rotation, and filename changes.

Error handling includes retry logic for file descriptor exhaustion (ENFILE/EMFILE) and automatic disabling of rotation for other errors. The function updates the last_file_name and logFile pointers to reflect the new file state upon successful rotation.

## Parameters / Member Variables
- `time_based_rotation`: Boolean indicating if rotation was triggered by time elapsed
- `size_rotation_for`: Bitmask indicating which destinations need size-based rotation
- `fntime`: Timestamp used for generating the new log filename
- `target_dest`: The specific log destination being rotated (LOG_DESTINATION_STDERR, LOG_DESTINATION_CSVLOG, or LOG_DESTINATION_JSONLOG)
- `last_file_name`: Pointer to string tracking the previous filename (updated on successful rotation)
- `logFile`: Pointer to FILE handle for the current log file (updated on successful rotation)

## Dependencies
- Functions called/Symbols referenced:
  - pg_time_t
  - LOG_DESTINATION_STDERR
  - LOG_DESTINATION_CSVLOG
  - LOG_DESTINATION_JSONLOG
  - [logfile_getname](logfile_getname.md)
  - [logfile_open](logfile_open.md)
- Called from (representative examples):
  - [logfile_rotate](logfile_rotate.md)

## Notes and Other Information
- Static function used internally within the syslogger module
- Returns false if rotation should be stopped, true to continue processing other formats
- Handles destination-specific file extensions (.csv for CSV logs, .json for JSON logs)
- Implements intelligent truncate vs append logic based on configuration and conditions
- Provides graceful error handling with automatic rotation disabling for persistent errors
- Memory management includes proper cleanup of filename strings using pfree()
- Special handling ensures stderr destination cannot be completely disabled

## Simplified Source

```c
// Simplified version of logfile_rotate_dest
static bool logfile_rotate_dest(bool time_based_rotation, int size_rotation_for,
                               pg_time_t fntime, int target_dest,
                               char **last_file_name, FILE **logFile) {
    char *logFileExt = NULL;
    char *filename;
    FILE *fh;

    // Step 1: Clean up if destination was turned off
    if ((Log_destination & target_dest) == 0 && target_dest != LOG_DESTINATION_STDERR) {
        if (*logFile != NULL)
            fclose(*logFile);
        *logFile = NULL;
        if (*last_file_name != NULL)
            pfree(*last_file_name);
        *last_file_name = NULL;
        return true;
    }

    // Step 2: Check if rotation is needed
    if (!time_based_rotation && (size_rotation_for & target_dest) == 0)
        return true;

    // Step 3: Determine file extension based on destination type
    if (target_dest == LOG_DESTINATION_CSVLOG)
        logFileExt = ".csv";
    else if (target_dest == LOG_DESTINATION_JSONLOG)
        logFileExt = ".json";
    // stderr uses no extension (logFileExt remains NULL)

    // Step 4: Generate new filename
    filename = logfile_getname(fntime, logFileExt);

    // Step 5: Open new file (truncate if conditions met, otherwise append)
    if (Log_truncate_on_rotation && time_based_rotation &&
        *last_file_name != NULL &&
        strcmp(filename, *last_file_name) != 0)
        fh = logfile_open(filename, "w", true);  // Truncate mode
    else
        fh = logfile_open(filename, "a", true);  // Append mode

    // Step 6: Handle file open failure
    if (!fh) {
        // Disable rotation for persistent errors (except file descriptor limits)
        if (errno != ENFILE && errno != EMFILE) {
            ereport(LOG, (errmsg("disabling automatic rotation (use SIGHUP to re-enable)")));
            rotation_disabled = true;
        }
        if (filename)
            pfree(filename);
        return false;
    }

    // Step 7: Update file handles and tracking information
    if (*logFile != NULL)
        fclose(*logFile);
    *logFile = fh;

    if (*last_file_name != NULL)
        pfree(*last_file_name);
    *last_file_name = filename;

    return true;
}
```

Key simplifications made:
- Removed detailed comments for clarity while preserving essential logic
- Added step-by-step comments to show the main workflow
- Consolidated similar conditional checks
- Focused on the main execution path
- Maintained all critical error handling and resource management
- Preserved the core algorithm for deciding between truncate vs append modes