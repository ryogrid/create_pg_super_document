# pg_log_v

## Location
[src/bin/pg_upgrade/util.c:176-258](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_upgrade/util.c#L176-L258)

## Overview
Core internal logging function that handles all log message formatting and output routing for pg_upgrade, with sophisticated terminal and verbosity-aware behavior.

## Definition

```c
static void
pg_log_v(eLogType type, const char *fmt, va_list ap)
```
## Detailed Description
The  function is the central logging engine for pg_upgrade, responsible for formatting and routing all log messages according to their type and the current output configuration. It implements sophisticated logic to handle different output destinations (TTY vs non-TTY) and verbosity levels.

Key functionality includes:
- **Message formatting**: Uses vsnprintf to format variadic arguments with internationalization support
- **Dual output routing**: Simultaneously handles internal log file output and console output
- **Terminal-aware formatting**: Adapts output format based on TTY detection for optimal user experience
- **Progress message handling**: Special formatting for status messages including truncation and carriage returns
- **Verbosity filtering**: Selectively outputs PG_VERBOSE and PG_STATUS messages based on verbose mode
- **Fatal error handling**: Provides special formatting for fatal errors and triggers program termination

The function implements different behaviors for each log type:
- PG_VERBOSE: Only shown in verbose mode
- PG_STATUS: Progress messages with special TTY formatting including left-truncation and carriage returns
- PG_REPORT_NONL: No-newline output for status preparation
- PG_REPORT/PG_WARNING: Standard messages with newlines
- PG_FATAL: Error messages with program termination

## Parameters / Member Variables
- `type`: Log message type from eLogType enumeration (PG_VERBOSE, PG_STATUS, PG_REPORT, etc.)
- `*fmt`: Printf-style format string for the message (should not end in newline)
- `ap`: va_list containing the variadic arguments for format string
## Dependencies
- Functions called/Symbols referenced:
  -  (formats the message with va_list arguments)
  -  (writes to internal log file)
  -  (writes to console)
  -  (flushes output buffers)
  -  (terminates program on fatal errors)
  -  (string length calculations)
  -  (log type enumeration)
  -  (message buffer size constant)
  -  (terminal output width constant)
  - Log level constants: , , , , , 
  -  (internal log file handle)
  -  (verbose mode flag)
  -  (TTY detection flag)
- Called from (representative examples):
  -  in src/bin/pg_upgrade/util.c:264
  -  in src/bin/pg_upgrade/util.c:275

## Notes and Other Information
- Static function serving as the internal implementation for all public logging functions
- Includes assertions to validate format string requirements (no trailing newlines)
- Implements intelligent message truncation for status messages that exceed terminal width
- The "..." prefix indicates when left-truncation has occurred for long status messages
- Fatal errors include an extra newline to ensure clean output even when interrupting status messages
- All output is immediately flushed to ensure real-time feedback during long operations
- Supports internationalization through the _() macro for message translation

## Simplified Source

```c
static void pg_log_v(eLogType type, const char *fmt, va_list ap) {
    char message[QUERY_ALLOC];

    // Format the message with variadic arguments
    vsnprintf(message, sizeof(message), _(fmt), ap);

    // Write to internal log file (if open and appropriate verbosity)
    if (((type != PG_VERBOSE && type != PG_STATUS) || log_opts.verbose) &&
        log_opts.internal != NULL) {

        if (type == PG_STATUS)
            fprintf(log_opts.internal, "  %s\n", message);  // Status gets indent
        else if (type == PG_REPORT_NONL)
            fprintf(log_opts.internal, "%s", message);      // No newline
        else
            fprintf(log_opts.internal, "%s\n", message);    // Standard format

        fflush(log_opts.internal);
    }

    // Handle console output based on message type
    switch (type) {
        case PG_VERBOSE:
            if (log_opts.verbose)
                printf("%s\n", message);
            break;

        case PG_STATUS:
            // Progress messages: special formatting for terminals
            if (log_opts.isatty) {
                // Truncate long messages and use carriage return for overwriting
                bool fits = (strlen(message) <= MESSAGE_WIDTH - 2);
                printf("  %s%-*.*s\r",
                       fits ? "" : "...",
                       MESSAGE_WIDTH - 2, MESSAGE_WIDTH - 2,
                       fits ? message : message + strlen(message) - MESSAGE_WIDTH + 5);
            } else if (log_opts.verbose) {
                printf("  %s\n", message);
            }
            break;

        case PG_REPORT_NONL:
            printf("%s", message);  // No newline for status preparation
            break;

        case PG_REPORT:
        case PG_WARNING:
            printf("%s\n", message);
            break;

        case PG_FATAL:
            // Fatal errors: extra newline and exit
            printf("\n%s\n", message);
            printf(_("Failure, exiting\n"));
            exit(1);
            break;
    }

    fflush(stdout);
}
```