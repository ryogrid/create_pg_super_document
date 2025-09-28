# log_status_format

## Location
[src/backend/utils/error/elog.c:2813-3165](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/error/elog.c#L2813-L3165)

## Overview
A comprehensive function that formats log status information by interpreting format escape sequences and appending contextual data to a buffer.

## Definition
```c
void log_status_format(StringInfo buf, const char *format, ErrorData *edata)
```

## Detailed Description
The `log_status_format` function processes a format string containing escape sequences (similar to printf-style formatting) and replaces them with contextual information about the current PostgreSQL process, session, transaction, and error state. This is the core formatting engine used for customizable log prefixes and JSON log formatting.

The function maintains static variables to track line numbers and process identity across calls, ensuring consistent numbering and proper reset behavior when the process ID changes. It supports a rich set of format specifiers that can include padding information for alignment.

## Parameters / Member Variables
- `buf`: StringInfo buffer where the formatted output will be appended
- `format`: Format string containing literal text and escape sequences (e.g., "%t %u %d [%p] ")
- `edata`: ErrorData structure containing error context and metadata

## Dependencies
- Functions called/Symbols referenced:
  - [reset_formatted_start_time](../r/reset_formatted_start_time.md)
  - [process_log_prefix_padding](../p/process_log_prefix_padding.md)
  - [appendStringInfoSpaces](../a/appendStringInfoSpaces.md)
  - [get_backend_type_for_log](../g/get_backend_type_for_log.md)
  - [get_formatted_log_time](../g/get_formatted_log_time.md)
  - [get_formatted_start_time](../g/get_formatted_start_time.md)
  - [get_ps_display](../g/get_ps_display.md)
  - [GetTopTransactionIdIfAny](../G/GetTopTransactionIdIfAny.md)
  - [unpack_sql_state](../u/unpack_sql_state.md)
  - [pgstat_get_my_query_id](../p/pgstat_get_my_query_id.md)
  - Various StringInfo manipulation functions
- Called from:
  - [log_line_prefix](log_line_prefix.md)
  - LOG_DESTINATION_JSONLOG (via header reference)

## Notes and Other Information
- Supports extensive format specifiers: %a (application), %b (backend type), %c (session ID), %d (database), %e (SQL state), %h (remote host), %i (command tag), %l (line number), %m (timestamp with milliseconds), %n (timestamp with milliseconds since epoch), %p (process ID), %P (parallel leader PID), %q (stop processing in postmaster), %r (remote host with port), %s (process start time), %t (timestamp), %u (username), %v (virtual transaction ID), %x (transaction ID), %Q (query ID)
- Includes padding support for alignment (e.g., %10u for right-aligned username in 10 characters)
- Maintains per-process line numbering with automatic reset on process change
- Critical for PostgreSQL's flexible logging system configuration

## Simplified Source

```c
// Simplified version of log_status_format
void log_status_format(StringInfo buf, const char *format, ErrorData *edata) {
    // Static counters for line numbers and process tracking
    static long log_line_number = 0;
    static int log_my_pid = 0;
    int padding;
    const char *p;

    // Reset line counter when process changes
    if (log_my_pid != MyProcPid) {
        log_line_number = 0;
        log_my_pid = MyProcPid;
        reset_formatted_start_time();
    }
    log_line_number++;

    if (format == NULL)
        return;

    // Parse format string character by character
    for (p = format; *p != '\0'; p++) {
        if (*p != '%') {
            // Copy literal characters directly
            appendStringInfoChar(buf, *p);
            continue;
        }

        // Handle escape sequences starting with '%'
        p++;
        if (*p == '\0')
            break;
        if (*p == '%') {
            appendStringInfoChar(buf, '%');
            continue;
        }

        // Extract padding information if present
        if (*p > '9')
            padding = 0;
        else if ((p = process_log_prefix_padding(p, &padding)) == NULL)
            break;

        // Process format specifiers
        switch (*p) {
            case 'a': // Application name
                append_application_name(buf, padding);
                break;
            case 'b': // Backend type
                append_backend_type(buf, padding);
                break;
            case 'u': // Username
                append_username(buf, padding);
                break;
            case 'd': // Database name
                append_database_name(buf, padding);
                break;
            case 'c': // Session ID (timestamp.pid)
                append_session_id(buf, padding);
                break;
            case 'p': // Process ID
                append_process_id(buf, padding);
                break;
            case 'P': // Parallel leader PID
                append_parallel_leader_pid(buf, padding);
                break;
            case 'l': // Line number
                append_line_number(buf, padding, log_line_number);
                break;
            case 'm': // Timestamp with milliseconds
                append_formatted_timestamp(buf, padding);
                break;
            case 't': // Standard timestamp
                append_standard_timestamp(buf, padding);
                break;
            case 'n': // Timestamp since epoch
                append_epoch_timestamp(buf, padding);
                break;
            case 's': // Process start time
                append_start_time(buf, padding);
                break;
            case 'i': // Command tag
                append_command_tag(buf, padding);
                break;
            case 'r': // Remote host with port
                append_remote_host_with_port(buf, padding);
                break;
            case 'h': // Remote host only
                append_remote_host(buf, padding);
                break;
            case 'q': // Stop processing in postmaster
                if (MyProcPort == NULL)
                    return;
                break;
            case 'v': // Virtual transaction ID
                append_virtual_xid(buf, padding);
                break;
            case 'x': // Transaction ID
                append_transaction_id(buf, padding);
                break;
            case 'e': // SQL error state
                append_sql_state(buf, padding, edata);
                break;
            case 'Q': // Query ID
                append_query_id(buf, padding);
                break;
            default:
                // Unknown format specifier - ignore
                break;
        }
    }
}
```

Key simplifications made:
- Abstracted repetitive padding and string appending logic into helper function calls
- Removed inline implementations of format specifiers for clarity
- Consolidated similar cases that handle padding and string formatting
- Simplified complex conditional logic while preserving core functionality
- Maintained the essential format parsing and dispatch mechanism
- Preserved critical static variable handling for line numbering and process tracking