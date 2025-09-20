# update_metainfo_datafile

## Location
[src/backend/postmaster/syslogger.c:1476-1572](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/syslogger.c#L1476-L1572)

## Overview
Maintains a metadata file containing the current names of active log files, enabling external tools to identify current log files during time-based rotation.

## Definition

```c
struct stat stat_buf;
```
## Detailed Description
The  function creates and maintains a metadata file () that contains information about the currently active log files for each configured log destination. This is particularly useful when time-based log rotation is enabled, as it allows external monitoring tools and scripts to determine which log files are currently being written to. The function uses atomic file operations (write to temporary file, then rename) to ensure the metadata file is always in a consistent state. If no log destinations are active, it removes the metadata file entirely.

## Parameters / Member Variables
- This function takes no parameters and operates on global state

## Dependencies
- Functions called/Symbols referenced:
  -  - Remove metadata file when no log destinations active
  - 0022 - Set file permissions for new metadata file
  -  - Open temporary metadata file for writing
  -  - Set line buffering for file output
  -  - Write log destination entries to metadata file
  -  - Close metadata file
  -  - Atomically replace old metadata file with new one
  -  - PostgreSQL error reporting function
  -  - Error code for file access errors
- Constants used:
  -  - Final metadata filename ("current_logfiles")
  -  - Temporary metadata filename
  - , ,  - Log destination flags
  -  - PostgreSQL line buffering constant
- Global variables used:
  -  - Bitmask of active log destinations
  -  - Current stderr log filename
  -  - Current CSV log filename  
  -  - Current JSON log filename
  -  - PostgreSQL file permission mask
- Called from (representative examples):
  -  - During logger initialization and configuration changes
  -  - After successful log rotation to update current filenames

## Notes and Other Information
- Uses atomic file operations (write to temp file, then rename) for consistency
- Removes metadata file entirely if no file-based log destinations are active
- File format: one line per active destination ("stderr filename", "csvlog filename", "jsonlog filename")
- Uses same permissions as PostgreSQL data directory for the metadata file
- Handles platform-specific line endings (CRLF on Windows via )
- Critical for external monitoring tools that need to track current log files during rotation
- All file operations include comprehensive error reporting and cleanup on failure