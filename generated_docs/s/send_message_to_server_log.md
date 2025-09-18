# send_message_to_server_log

## Location
src/backend/utils/error/elog.c: 3186 - 3425

## Overview
Central function responsible for formatting and routing error/log messages to various server-side logging destinations based on configuration.

## Definition
```c
static void send_message_to_server_log(ErrorData *edata)
```

## Detailed Description
The `send_message_to_server_log` function is the primary dispatcher for server-side logging in PostgreSQL. It takes error data and formats it into a comprehensive log message, then routes it to one or more configured destinations (stderr, syslog, Windows Event Log, CSV log, JSON log).

The function builds a formatted message by:
1. Adding a configurable line prefix using `log_line_prefix`
2. Including the error severity and SQL state (if verbose mode is enabled)
3. Adding the main error message with cursor position if applicable
4. Appending additional details like DETAIL, HINT, QUERY, CONTEXT, LOCATION, BACKTRACE, and STATEMENT based on verbosity settings
5. Routing the formatted message to appropriate destinations based on `Log_destination` flags

The function handles fallback scenarios where certain log destinations are unavailable and ensures messages reach stderr when necessary.

## Parameters / Member Variables
- `edata`: ErrorData structure containing comprehensive error information including severity, message, details, context, location info, etc.

## Dependencies
- Functions called/Symbols referenced:
  - log_line_prefix
  - error_severity
  - unpack_sql_state
  - append_with_tabs
  - check_log_of_query
  - write_syslog
  - write_eventlog
  - write_csvlog
  - write_jsonlog
  - write_pipe_chunks
  - write_console
  - write_syslogger_file
  - Various logging constants and backend type checks
- Called from:
  - EmitErrorReport

## Notes and Other Information
- This is a static function internal to elog.c, serving as the core server logging implementation
- Supports multiple simultaneous log destinations with appropriate formatting for each
- Handles platform-specific logging (syslog on Unix, Event Log on Windows)
- Implements fallback logic when structured logging destinations are unavailable
- Uses chunking protocol for stderr output when syslogger is active
- Critical for PostgreSQL's configurable and comprehensive server-side logging system
- Message formatting depends on `Log_error_verbosity` setting (TERSE, DEFAULT, VERBOSE)