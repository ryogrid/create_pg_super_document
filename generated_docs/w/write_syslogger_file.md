# write_syslogger_file

## Location
[src/backend/postmaster/syslogger.c:1094-1140](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/syslogger.c#L1094-L1140)

## Overview
Writes text data to the currently open log file, supporting multiple log destinations including CSV and JSON structured logs.

## Definition
```c
void write_syslogger_file(const char *buffer, int count, int destination)
```

## Detailed Description
This function serves as the central log writing mechanism for the syslogger process. It intelligently routes log data to the appropriate log file based on the destination parameter. The function handles multiple log formats including regular syslog, CSV structured logs, and JSON structured logs.

The function implements fallback logic: if a structured log file (CSV or JSON) is requested but not available, it writes to the regular syslog file instead. This prevents log data loss during configuration changes or file opening failures. The function is designed to avoid recursion issues that could occur during error handling by using write_stderr for error reporting instead of the standard ereport mechanism.

This function is exported specifically so that elog.c can call it when MyBackendType is B_LOGGER, allowing the syslogger process itself to record its own log messages even though its stderr doesnt point to the syslog pipe.

## Simplified Source

```c
// Simplified version of write_syslogger_file
void write_syslogger_file(const char *buffer, int count, int destination) {
    FILE *logfile;
    int bytes_written;

    // Step 1: Choose the appropriate log file based on destination
    if ((destination & LOG_DESTINATION_CSVLOG) && csvlogFile != NULL) {
        logfile = csvlogFile;  // Use CSV log file if requested and available
    } else if ((destination & LOG_DESTINATION_JSONLOG) && jsonlogFile != NULL) {
        logfile = jsonlogFile; // Use JSON log file if requested and available
    } else {
        logfile = syslogFile;  // Fallback to regular syslog file
    }

    // Step 2: Write the buffer to the chosen log file
    bytes_written = fwrite(buffer, 1, count, logfile);

    // Step 3: Handle write failures
    if (bytes_written != count) {
        write_stderr("could not write to log file: %m\n");
    }
}
```

Key simplifications made:
- Removed detailed comments explaining race conditions and design decisions
- Simplified variable names for clarity (rc → bytes_written)
- Added step-by-step comments for the main logic flow
- Consolidated the destination selection logic with clearer inline comments
- Preserved the essential algorithm: file selection → write → error handling