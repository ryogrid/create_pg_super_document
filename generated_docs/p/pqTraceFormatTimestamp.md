# pqTraceFormatTimestamp

## Location
src/interfaces/libpq/fe-trace.c: 80 - 105

## Overview
Formats the current time with microsecond precision into a caller-supplied buffer for use in trace output timestamps.

## Definition
```c
static void pqTraceFormatTimestamp(char *timestr, size_t ts_len)
```

## Detailed Description
pqTraceFormatTimestamp is an internal utility function that generates formatted timestamp strings for libpq's protocol tracing output. It provides high-precision timing information by capturing both seconds and microseconds, formatting them in a human-readable ISO-8601-style format. The function is designed to be portable across different platforms, including special handling for MSVC's timeval implementation. The timestamp format includes date, time to the second, and microsecond precision, making it suitable for detailed protocol timing analysis.

## Parameters / Member Variables
- `timestr`: Character buffer where the formatted timestamp will be written
- `ts_len`: Size of the timestr buffer to prevent overflow

## Dependencies
- Functions called/Symbols referenced:
  - gettimeofday (system call to get current time)
  - strftime (formats the date/time portion)
  - localtime (converts time_t to local time structure)
  - strlen, snprintf (string manipulation functions)
- Called from (representative examples):
  - pqTraceOutputMessage (line 526 in fe-trace.c)
  - pqTraceOutputNoTypeByteMessage (line 705 in fe-trace.c)

## Notes and Other Information
- Static function - internal to fe-trace.c module
- Handles MSVC compatibility by explicitly casting tv_sec to time_t
- Output format: "YYYY-MM-DD HH:MM:SS.uuuuuu" (with microseconds)
- Based on PostgreSQL's get_formatted_log_time but simplified for tracing needs
- Provides microsecond precision for detailed protocol timing analysis
- Uses safe string functions (strftime, snprintf) to prevent buffer overflows
- Essential component of libpq's timestamped trace output functionality