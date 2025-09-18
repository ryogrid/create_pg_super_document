# pg_log_filter_error

## Location
src/bin/pg_dump/filter.c: 155 - 179

## Overview
Logs filter parsing errors with context information including the source (file or stdin) and line number where the error occurred.

## Definition
```c
void pg_log_filter_error(FilterStateData *fstate, const char *fmt, ...)
```

## Detailed Description
This function provides a standardized way to report errors encountered while parsing filter files in pg_dump utilities. It accepts printf-style format strings and arguments, formats them into a buffer, and then logs an error message that includes contextual information about where the error occurred. The function differentiates between errors from stdin and from named files, providing appropriate error messages for each case.

## Parameters / Member Variables
- `fstate`: Pointer to FilterStateData structure containing context information
- `fmt`: Printf-style format string for the error message
- `...`: Variable arguments corresponding to the format string

## Dependencies
- Functions called/Symbols referenced:
  - va_start, va_end (variable argument macros)
  - vsnprintf
  - pg_log_error
- Called from (representative examples):
  - read_quoted_string (in filter.c)
  - read_pattern (in filter.c)
  - filter_read_item (in filter.c)
  - read_dump_filters (in pg_dump.c)
  - read_dumpall_filters (in pg_dumpall.c)
  - read_restore_filters (in pg_restore.c)

## Notes and Other Information
- Uses a fixed-size buffer (256 bytes) for formatting the error message
- Provides different error message formats for stdin vs. file input to give users appropriate context
- Includes line number information to help users locate the problematic filter entry
- Part of the error handling infrastructure for the filter parsing system
- Supports variable argument lists like printf for flexible error message formatting
- Used extensively throughout the filter parsing code to provide consistent error reporting