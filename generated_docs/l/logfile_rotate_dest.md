# logfile_rotate_dest

## Location
src/backend/postmaster/syslogger.c: 1263 - 1361

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