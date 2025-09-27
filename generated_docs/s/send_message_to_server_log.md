# send_message_to_server_log

## Location
[src/backend/utils/error/elog.c:3186-3425](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/error/elog.c#L3186-L3425)

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
  - [log_line_prefix](../l/log_line_prefix.md)
  - [error_severity](../e/error_severity.md)
  - [unpack_sql_state](../u/unpack_sql_state.md)
  - [append_with_tabs](../a/append_with_tabs.md)
  - [check_log_of_query](../c/check_log_of_query.md)
  - [write_syslog](../w/write_syslog.md)
  - [write_eventlog](../w/write_eventlog.md)
  - [write_csvlog](../w/write_csvlog.md)
  - [write_jsonlog](../w/write_jsonlog.md)
  - [write_pipe_chunks](../w/write_pipe_chunks.md)
  - [write_console](../w/write_console.md)
  - [write_syslogger_file](../w/write_syslogger_file.md)
  - Various logging constants and backend type checks
- Called from:
  - [EmitErrorReport](../E/EmitErrorReport.md)

## Notes and Other Information
- This is a static function internal to elog.c, serving as the core server logging implementation
- Supports multiple simultaneous log destinations with appropriate formatting for each
- Handles platform-specific logging (syslog on Unix, Event Log on Windows)
- Implements fallback logic when structured logging destinations are unavailable
- Uses chunking protocol for stderr output when syslogger is active
- Critical for PostgreSQL's configurable and comprehensive server-side logging system
- Message formatting depends on `Log_error_verbosity` setting (TERSE, DEFAULT, VERBOSE)

## Simplified Source

```c
// Simplified version of send_message_to_server_log
static void send_message_to_server_log(ErrorData *error_data) {
    StringInfoData message_buffer;
    bool need_stderr_fallback = false;

    // Step 1: Initialize message buffer and build basic message
    initStringInfo(&message_buffer);

    // Add line prefix (timestamp, pid, etc.) and severity
    log_line_prefix(&message_buffer, error_data);
    appendStringInfo(&message_buffer, "%s: ", error_severity(error_data->elevel));

    // Add SQL state if verbose logging enabled
    if (Log_error_verbosity >= PGERROR_VERBOSE) {
        appendStringInfo(&message_buffer, "%s: ", unpack_sql_state(error_data->sqlerrcode));
    }

    // Add main error message with cursor position if available
    if (error_data->message) {
        append_with_tabs(&message_buffer, error_data->message);
    } else {
        append_with_tabs(&message_buffer, "missing error text");
    }

    if (error_data->cursorpos > 0 || error_data->internalpos > 0) {
        appendStringInfo(&message_buffer, " at character %d",
                        error_data->cursorpos > 0 ? error_data->cursorpos : error_data->internalpos);
    }
    appendStringInfoChar(&message_buffer, '\n');

    // Step 2: Add detailed information based on verbosity level
    if (Log_error_verbosity >= PGERROR_DEFAULT) {
        // Add DETAIL, HINT, QUERY, CONTEXT sections
        add_optional_sections(&message_buffer, error_data);

        // Add location info if verbose
        if (Log_error_verbosity >= PGERROR_VERBOSE && error_data->funcname && error_data->filename) {
            log_line_prefix(&message_buffer, error_data);
            appendStringInfo(&message_buffer, "LOCATION: %s, %s:%d\n",
                           error_data->funcname, error_data->filename, error_data->lineno);
        }

        // Add backtrace if available
        if (error_data->backtrace) {
            log_line_prefix(&message_buffer, error_data);
            appendStringInfoString(&message_buffer, "BACKTRACE: ");
            append_with_tabs(&message_buffer, error_data->backtrace);
            appendStringInfoChar(&message_buffer, '\n');
        }
    }

    // Step 3: Add SQL statement if logging is enabled for this query
    if (check_log_of_query(error_data)) {
        log_line_prefix(&message_buffer, error_data);
        appendStringInfoString(&message_buffer, "STATEMENT: ");
        append_with_tabs(&message_buffer, debug_query_string);
        appendStringInfoChar(&message_buffer, '\n');
    }

    // Step 4: Route message to configured destinations
    write_to_configured_destinations(error_data, &message_buffer, &need_stderr_fallback);

    // Step 5: Write to stderr if configured or fallback needed
    if ((Log_destination & LOG_DESTINATION_STDERR) || need_stderr_fallback) {
        write_to_stderr_destination(&message_buffer);
    }

    // Step 6: Special handling for syslogger process
    if (MyBackendType == B_LOGGER) {
        write_syslogger_file(message_buffer.data, message_buffer.len, LOG_DESTINATION_STDERR);
    }

    // Cleanup
    pfree(message_buffer.data);
}

// Helper function for adding optional message sections
static void add_optional_sections(StringInfoData *buffer, ErrorData *error_data) {
    // Add DETAIL section
    if (error_data->detail_log || error_data->detail) {
        log_line_prefix(buffer, error_data);
        appendStringInfoString(buffer, "DETAIL: ");
        append_with_tabs(buffer, error_data->detail_log ? error_data->detail_log : error_data->detail);
        appendStringInfoChar(buffer, '\n');
    }

    // Add HINT section
    if (error_data->hint) {
        log_line_prefix(buffer, error_data);
        appendStringInfoString(buffer, "HINT: ");
        append_with_tabs(buffer, error_data->hint);
        appendStringInfoChar(buffer, '\n');
    }

    // Add QUERY section
    if (error_data->internalquery) {
        log_line_prefix(buffer, error_data);
        appendStringInfoString(buffer, "QUERY: ");
        append_with_tabs(buffer, error_data->internalquery);
        appendStringInfoChar(buffer, '\n');
    }

    // Add CONTEXT section
    if (error_data->context && !error_data->hide_ctx) {
        log_line_prefix(buffer, error_data);
        appendStringInfoString(buffer, "CONTEXT: ");
        append_with_tabs(buffer, error_data->context);
        appendStringInfoChar(buffer, '\n');
    }
}

// Helper function for writing to various destinations
static void write_to_configured_destinations(ErrorData *error_data, StringInfoData *buffer, bool *fallback_needed) {
    // Write to syslog if enabled (Unix systems)
    if (Log_destination & LOG_DESTINATION_SYSLOG) {
        int syslog_level = map_postgres_level_to_syslog(error_data->elevel);
        write_syslog(syslog_level, buffer->data);
    }

    // Write to Windows Event Log if enabled
    if (Log_destination & LOG_DESTINATION_EVENTLOG) {
        write_eventlog(error_data->elevel, buffer->data, buffer->len);
    }

    // Write to CSV log if enabled and safe
    if (Log_destination & LOG_DESTINATION_CSVLOG) {
        if (redirection_done || MyBackendType == B_LOGGER) {
            write_csvlog(error_data);
        } else {
            *fallback_needed = true;
        }
    }

    // Write to JSON log if enabled and safe
    if (Log_destination & LOG_DESTINATION_JSONLOG) {
        if (redirection_done || MyBackendType == B_LOGGER) {
            write_jsonlog(error_data);
        } else {
            *fallback_needed = true;
        }
    }
}

// Helper function for stderr output
static void write_to_stderr_destination(StringInfoData *buffer) {
    if (redirection_done && MyBackendType != B_LOGGER) {
        // Use chunking protocol for syslogger
        write_pipe_chunks(buffer->data, buffer->len, LOG_DESTINATION_STDERR);
    } else if (pgwin32_is_service()) {
        // Windows service environment
        write_eventlog(error_level, buffer->data, buffer->len);
    } else {
        // Direct console output
        write_console(buffer->data, buffer->len);
    }
}
```

Key simplifications made:
- Extracted helper functions to break down the large monolithic function
- Consolidated similar conditional blocks into unified logic flows
- Abstracted the complex syslog level mapping into a helper function
- Simplified the destination routing logic into a dedicated helper
- Removed platform-specific #ifdef blocks for clarity (noted in comments)
- Focused on the main execution path while preserving all essential functionality
- Added descriptive variable names and step-by-step comments
- Maintained the core algorithm: build message → route to destinations → cleanup