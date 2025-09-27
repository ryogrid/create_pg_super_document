# logfile_open

## Location
[src/backend/postmaster/syslogger.c:1218-1262](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/syslogger.c#L1218-L1262)

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
  - [SysLogger_Start](../S/SysLogger_Start.md)
  - [logfile_rotate_dest](logfile_rotate_dest.md)

## Notes and Other Information
- Static function used internally within the syslogger module
- Automatically preserves owner write permissions regardless of Log_file_mode setting
- Uses line buffering to ensure timely log output
- Platform-specific: enables text mode with CRLF line endings on Windows
- Supports both fatal and non-fatal error handling modes
- Temporarily modifies process umask during file creation for proper permissions

## Simplified Source

```c
// Simplified version of logfile_open
static FILE *logfile_open(const char *filename, const char *mode, bool allow_errors) {
    FILE *fh;
    mode_t original_umask;

    // Step 1: Set proper file permissions by temporarily changing umask
    // Ensure owner write permission is always preserved
    original_umask = umask((~(Log_file_mode | S_IWUSR)) & (S_IRWXU | S_IRWXG | S_IRWXO));

    // Step 2: Open the file
    fh = fopen(filename, mode);

    // Step 3: Restore original umask
    umask(original_umask);

    // Step 4: Configure file if successfully opened
    if (fh) {
        // Set line buffering for timely output
        setvbuf(fh, NULL, PG_IOLBF, 0);

        // Platform-specific: Use CRLF line endings on Windows
#ifdef WIN32
        _setmode(_fileno(fh), _O_TEXT);
#endif
    } else {
        // Step 5: Handle file open failure
        int saved_errno = errno;

        // Report error based on allow_errors flag
        ereport(allow_errors ? LOG : FATAL,
                (errcode_for_file_access(),
                 errmsg("could not open log file \"%s\": %m", filename)));

        // Preserve original errno for caller
        errno = saved_errno;
    }

    return fh;
}
```

Key simplifications made:
- Added clear step-by-step comments explaining the main operations
- Used more descriptive variable name (`original_umask` instead of `oumask`)
- Grouped related operations into logical steps
- Simplified the error handling explanation
- Maintained the essential umask manipulation and file configuration logic
- Preserved platform-specific Windows handling