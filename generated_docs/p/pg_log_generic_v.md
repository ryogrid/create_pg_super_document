# pg_log_generic_v

## Location
[src/common/logging.c:216-334](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/logging.c#L216-L334)

## Overview
The core logging function in PostgreSQL's common logging system that handles formatted message output with level filtering, callback execution, and various formatting options.

## Definition

```c
void
pg_log_generic_v(enum pg_log_level level, enum pg_log_part part,
				 const char *pg_restrict fmt, va_list ap)
```
## Detailed Description
This is the fundamental logging function that implements PostgreSQL's common logging infrastructure. It performs level-based filtering, executes registered callbacks, handles internationalization, formats messages with appropriate prefixes and styling (including ANSI color codes), and outputs to stderr. The function manages memory allocation for message formatting, handles error conditions gracefully, and provides consistent formatting across all PostgreSQL components. It supports multi-part messages (primary, detail, hint), location information, and both terse and verbose output modes.

## Parameters / Member Variables
- : An enumeration value from  specifying the severity level (error, warning, info, debug) - messages below the global  threshold are filtered out
- : An enumeration value from  specifying the message part type (PG_LOG_PRIMARY, PG_LOG_DETAIL, PG_LOG_HINT) which affects formatting
- : A printf-style format string for the message content (restricted pointer, should not end with newline)
- : A va_list containing the variable arguments for the format string

## Dependencies
- Functions called/Symbols referenced:
  - pg_log_level, pg_log_part (enum types)
  - [log_pre_callback](../l/log_pre_callback.md), log_locus_callback (global callback functions)
  - PG_LOG_FLAG_TERSE (logging flag for terse mode)
  - ANSI_ESCAPE_FMT, ANSI_ESCAPE_RESET (ANSI color formatting)
  - UINT64_FORMAT (format macro for 64-bit integers)
  - PG_LOG_PRIMARY, PG_LOG_DETAIL, PG_LOG_HINT (message part constants)
  - PG_LOG_ERROR, PG_LOG_WARNING (log level constants)
  - vsnprintf, pg_malloc_extended, MCXT_ALLOC_NO_OOM
  - vfprintf, fprintf, fflush
- Called from (representative examples):
  - [pg_log_generic](pg_log_generic.md) (src/common/logging.c:211)
  - [report_manifest_error](../r/report_manifest_error.md) (src/bin/pg_combinebackup/load_manifest.c:233)
  - [warn_or_exit_horribly](../w/warn_or_exit_horribly.md) (src/bin/pg_dump/pg_backup_archiver.c:1914)
  - [walsummary_error_callback](../w/walsummary_error_callback.md) (src/bin/pg_walsummary/pg_walsummary.c:236)

## Notes and Other Information
- This function is the backbone of PostgreSQL's logging system and is used across all major components
- Implements level-based filtering to avoid unnecessary formatting overhead for suppressed messages
- Handles memory allocation gracefully with fallback behavior when memory is unavailable
- Supports ANSI color codes for different message types when appropriate SGR variables are set
- Flushes stdout before stderr output to ensure proper synchronization in buffered environments
- Automatically strips trailing newlines from messages to ensure consistent formatting
- The function preserves errno to avoid interfering with error handling in calling code
- Internationalization support via the _() macro for translating format strings
- Location information (filename:line) is displayed when provided via the locus callback

## Simplified Source

```c
// Simplified version of pg_log_generic_v
void pg_log_generic_v(enum pg_log_level level, enum pg_log_part part,
                      const char *pg_restrict fmt, va_list ap) {
    // Save errno to restore later
    int save_errno = errno;
    const char *filename = NULL;
    uint64 lineno = 0;

    // Validate inputs
    Assert(progname && level && fmt);
    Assert(fmt[strlen(fmt) - 1] != '\n');  // No trailing newline

    // Early exit if log level is too low
    if (level < __pg_log_level) {
        return;
    }

    // Flush stdout for proper synchronization
    fflush(stdout);

    // Execute pre-logging callback if set
    if (log_pre_callback) {
        log_pre_callback();
    }

    // Get location information if callback is set
    if (log_locus_callback) {
        log_locus_callback(&filename, &lineno);
    }

    // Translate format string for internationalization
    fmt = _(fmt);

    // Output location prefix (program name, file, line)
    if (!(log_flags & PG_LOG_FLAG_TERSE) || filename) {
        // Apply locus coloring if available
        if (sgr_locus) {
            fprintf(stderr, ANSI_ESCAPE_FMT, sgr_locus);
        }

        // Print program name unless in terse mode
        if (!(log_flags & PG_LOG_FLAG_TERSE)) {
            fprintf(stderr, "%s:", progname);
        }

        // Print filename and line number
        if (filename) {
            fprintf(stderr, "%s:", filename);
            if (lineno > 0) {
                fprintf(stderr, UINT64_FORMAT ":", lineno);
            }
        }

        fprintf(stderr, " ");

        // Reset color formatting
        if (sgr_locus) {
            fprintf(stderr, ANSI_ESCAPE_RESET);
        }
    }

    // Output message type prefix (error:, warning:, detail:, hint:)
    if (!(log_flags & PG_LOG_FLAG_TERSE)) {
        switch (part) {
            case PG_LOG_PRIMARY:
                if (level == PG_LOG_ERROR) {
                    if (sgr_error) fprintf(stderr, ANSI_ESCAPE_FMT, sgr_error);
                    fprintf(stderr, _("error: "));
                    if (sgr_error) fprintf(stderr, ANSI_ESCAPE_RESET);
                } else if (level == PG_LOG_WARNING) {
                    if (sgr_warning) fprintf(stderr, ANSI_ESCAPE_FMT, sgr_warning);
                    fprintf(stderr, _("warning: "));
                    if (sgr_warning) fprintf(stderr, ANSI_ESCAPE_RESET);
                }
                break;
            case PG_LOG_DETAIL:
                if (sgr_note) fprintf(stderr, ANSI_ESCAPE_FMT, sgr_note);
                fprintf(stderr, _("detail: "));
                if (sgr_note) fprintf(stderr, ANSI_ESCAPE_RESET);
                break;
            case PG_LOG_HINT:
                if (sgr_note) fprintf(stderr, ANSI_ESCAPE_FMT, sgr_note);
                fprintf(stderr, _("hint: "));
                if (sgr_note) fprintf(stderr, ANSI_ESCAPE_RESET);
                break;
        }
    }

    // Format and output the actual message
    errno = save_errno;  // Restore errno before formatting

    // Calculate required buffer size
    va_list ap2;
    va_copy(ap2, ap);
    size_t required_len = vsnprintf(NULL, 0, fmt, ap2) + 1;
    va_end(ap2);

    // Allocate buffer for formatted message
    char *buf = pg_malloc_extended(required_len, MCXT_ALLOC_NO_OOM);

    if (!buf) {
        // Fallback: print directly if memory allocation fails
        vfprintf(stderr, fmt, ap);
        return;
    }

    // Format the message
    errno = save_errno;  // Restore errno again
    vsnprintf(buf, required_len, fmt, ap);

    // Strip trailing newline for consistency
    if (required_len >= 2 && buf[required_len - 2] == '\n') {
        buf[required_len - 2] = '\0';
    }

    // Output final message with newline
    fprintf(stderr, "%s\n", buf);

    // Cleanup
    free(buf);
}
```

Key simplifications made:
- Added inline comments explaining each major section
- Consolidated ANSI color handling into clear blocks
- Simplified the message type prefix handling
- Made the memory allocation and fallback strategy more explicit
- Clarified the errno preservation strategy
- Grouped related operations (location formatting, message type prefixes, etc.)
- Maintained all original functionality while improving readability
- Highlighted the multi-step process: filtering, callbacks, formatting, output