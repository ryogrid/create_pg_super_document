# write_syslogger_file

## Location
src/backend/postmaster/syslogger.c: 1094 - 1140

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