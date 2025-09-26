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