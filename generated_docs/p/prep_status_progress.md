# prep_status_progress

## Location
[src/bin/pg_upgrade/util.c:156-175](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_upgrade/util.c#L156-L175)

## Overview
Displays formatted status messages for potentially long-running operations in pg_upgrade, with intelligent newline handling based on output mode.

## Definition

```c
void
prep_status_progress(const char *fmt,...)
```
## Detailed Description
The  function is a specialized variant of  designed specifically for operations that may take significant time to complete. It prepares the terminal for displaying progress updates during long-running processes.

Key behavioral differences from :
- **Intelligent newline handling**: When outputting to a TTY or in verbose mode, it appends a newline to move to the next line for progress items. In non-TTY/non-verbose mode, it behaves like  without newlines
- **Progress-aware formatting**: Designed to work with  calls that display individual progress items
- **Consistent width**: Like , messages are padded to MESSAGE_WIDTH for alignment

Typical usage pattern:


The function intelligently adapts its output format based on the destination and verbosity settings to ensure optimal user experience in both interactive and batch environments.

## Parameters / Member Variables
- : Printf-style format string describing the long-running operation about to begin
- : Variable arguments corresponding to format specifiers in fmt

## Dependencies
- Functions called/Symbols referenced:
  -  (formats the variadic arguments into a string)
  -  (outputs messages with PG_REPORT or PG_REPORT_NONL)
  -  (maximum string buffer size constant)
  -  (output formatting width constant)
  -  (log level with newline)
  -  (log level without newline)
  -  (TTY detection flag)
  -  (verbose mode flag)
- Called from (representative examples):
  -  in src/bin/pg_upgrade/dump.c:32
  -  in src/bin/pg_upgrade/pg_upgrade.c:540
  -  in src/bin/pg_upgrade/pg_upgrade.c:931
  -  in src/bin/pg_upgrade/relfilenumber.c:35

## Notes and Other Information
- Specialized for operations expected to show individual progress items (files being processed, objects being created, etc.)
- Works in conjunction with  and  to provide detailed progress feedback
- The conditional newline behavior ensures proper formatting across different output contexts
- Essential for providing user feedback during time-consuming pg_upgrade operations like file transfers and object creation

## Simplified Source

```c
void prep_status_progress(const char *fmt, ...) {
    va_list args;
    char message[MAX_STRING];

    // Format the message using variadic arguments
    va_start(args, fmt);
    vsnprintf(message, sizeof(message), fmt, args);
    va_end(args);

    // Choose output format based on TTY and verbose settings
    if (log_opts.isatty || log_opts.verbose)
        // TTY/verbose: include newline for progress items on next line
        pg_log(PG_REPORT, "%-*s", MESSAGE_WIDTH, message);
    else
        // Non-TTY/non-verbose: no newline for compact output
        pg_log(PG_REPORT_NONL, "%-*s", MESSAGE_WIDTH, message);
}
```