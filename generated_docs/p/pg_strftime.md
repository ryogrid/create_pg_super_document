# pg_strftime

## Location
src/timezone/strftime.c: 128 - 150

## Overview
PostgreSQL's version of the standard C library strftime function that converts a timestamp structure to a formatted string representation using pattern-based formatting.

## Definition

```c
size_t
pg_strftime(char *s, size_t maxsize, const char *format, const struct pg_tm *t)
```
## Detailed Description
pg_strftime is PostgreSQL's implementation of the strftime function for formatting timestamps. It takes a timestamp structure (pg_tm) and converts it to a string representation according to a specified format pattern. The function is designed to be similar to the standard C library strftime but uses PostgreSQL's own pg_tm structure and internal formatting logic through the _fmt function.

The function performs bounds checking to ensure the output doesn't exceed the buffer size and handles error conditions by setting appropriate errno values. It preserves the original errno value if no errors occur during processing.

## Parameters / Member Variables
- : Output buffer to store the formatted string
- : Maximum size of the output buffer (including null terminator)  
- : Format string containing conversion specifiers that define how the timestamp should be formatted
- : Pointer to a pg_tm structure containing the timestamp components to format

## Dependencies
- Functions called/Symbols referenced:
  - _fmt (core formatting function)
  - pg_tm (timestamp structure type)
  - IN_NONE (warning enumeration value)
  - EOVERFLOW (error code for buffer overflow)
  - ERANGE (error code for range error)
- Called from (representative examples):
  - str_time (in xlog.c)
  - build_backup_content (in xlogbackup.c)  
  - AddFileToBackupManifest (in backup_manifest.c)
  - timeofday (in timestamp.c)
  - get_formatted_log_time (in elog.c)

## Notes and Other Information
- Returns 0 on error (buffer overflow or range error) and sets errno appropriately
- Returns the number of characters written (excluding null terminator) on success
- The function preserves the original errno value when successful
- Uses PostgreSQL's internal _fmt function for the actual formatting work
- Commonly used throughout PostgreSQL for log formatting, backup operations, and timestamp display