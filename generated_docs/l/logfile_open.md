# logfile_open

## Location
src/backend/postmaster/syslogger.c: 1218 - 1262

## Overview
Opens a new log file with proper Unix permissions and buffering options, with configurable error handling behavior.

## Definition
```c
static FILE *logfile_open(const char *filename, const char *mode, bool allow_errors)
```

## Detailed Description
This function provides a centralized way to open log files with appropriate permissions, buffering, and platform-specific settings. It temporarily adjusts the process umask to ensure log files are created with the permissions specified by Log_file_mode, while preserving write permissions for the owner (IWUSR) regardless of the configuration.

The function sets up line buffering (PG_IOLBF) on successfully opened files to ensure timely log output. On Windows systems, it configures the file handle for text mode to use CRLF line endings. Error handling is configurable: when allow_errors is true, failures are logged and the function returns NULL; when false, failures are treated as fatal errors that terminate the process.

## Parameters / Member Variables
- `filename`: Path to the log file to be opened
- `mode`: File opening mode string (e.g., "a" for append, "w" for write)
- `allow_errors`: Boolean flag controlling error handling behavior - true for non-fatal errors, false for fatal errors

## Dependencies
- Functions called/Symbols referenced:
  - mode_t
  - S_IWUSR
  - S_IRWXU
  - S_IRWXG  
  - S_IRWXO
  - fopen
  - PG_IOLBF
- Called from (representative examples):
  - SysLogger_Start
  - logfile_rotate_dest

## Notes and Other Information
- Static function used internally within the syslogger module
- Automatically preserves owner write permissions regardless of Log_file_mode setting
- Uses line buffering to ensure timely log output
- Platform-specific: enables text mode with CRLF line endings on Windows
- Supports both fatal and non-fatal error handling modes
- Temporarily modifies process umask during file creation for proper permissions