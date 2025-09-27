# write_csvlog

## Location
[src/backend/utils/error/csvlog.c:63-262](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/error/csvlog.c#L63-L262)

## Overview
Generates and writes a comprehensive CSV-formatted log entry containing error/message data and process information according to PostgreSQL's structured logging format.

## Definition

```c
void
write_csvlog(ErrorData *edata)
```
## Detailed Description
This function constructs a detailed CSV log entry based on the provided ErrorData structure and current process state. It generates a standardized CSV format that includes timestamps, user/database information, process details, error specifics, and contextual data. The function maintains a static line counter per process, handles process ID changes (such as after fork), and formats all data according to PostgreSQL's CSV logging conventions. The resulting CSV line contains approximately 23 fields covering all aspects of the logged event, from basic identification to detailed error context.

## Parameters / Member Variables
- `edata`: Pointer to ErrorData structure containing error/message information including severity, SQL state, message text, and contextual details

## Dependencies
- Functions called/Symbols referenced:
  - [appendCSVLiteral](../a/appendCSVLiteral.md) (frequently used for CSV-formatting string fields)
  - [reset_formatted_start_time](../r/reset_formatted_start_time.md) (resets time formatting for new processes)
  - [get_formatted_log_time](../g/get_formatted_log_time.md) (gets current timestamp string)
  - [get_formatted_start_time](../g/get_formatted_start_time.md) (gets process start time string)
  - [get_ps_display](../g/get_ps_display.md) (gets process status display string)
  - [GetTopTransactionIdIfAny](../G/GetTopTransactionIdIfAny.md) (gets current transaction ID)
  - [error_severity](../e/error_severity.md) (converts error level to severity string)
  - [unpack_sql_state](../u/unpack_sql_state.md) (converts SQL error code to state string)
  - [check_log_of_query](../c/check_log_of_query.md) (determines if query should be logged)
  - [get_backend_type_for_log](../g/get_backend_type_for_log.md) (gets backend type string)
  - [pgstat_get_my_query_id](../p/pgstat_get_my_query_id.md) (gets current query ID)
  - [write_syslogger_file](write_syslogger_file.md) (writes directly to log file in syslogger process)
  - [write_pipe_chunks](write_pipe_chunks.md) (writes through pipe in other processes)
- Called from (representative examples):
  - [send_message_to_server_log](../s/send_message_to_server_log.md) (main error logging pathway)

## Notes and Other Information
- Maintains static per-process line numbering that resets when process ID changes
- Generates approximately 23 comma-separated fields in each log entry
- Handles NULL values gracefully by leaving fields empty
- Uses specialized formatting for remote host:port combinations
- Includes virtual transaction ID and transaction ID for database correlation
- Conditionally includes user query text based on logging policy
- Provides detailed error location information when verbose logging is enabled
- Supports both direct file writing (syslogger) and pipe-based logging (other processes)
- Output format is documented in PostgreSQL's configuration documentation
- Located in src/backend/utils/error/csvlog.c:63-262

## Simplified Source

```c
// Simplified version of write_csvlog
void write_csvlog(ErrorData *edata) {
    StringInfoData csv_buffer;
    static long log_line_number = 0;
    static int log_my_pid = 0;

    // Reset line counter when process changes (e.g., after fork)
    if (log_my_pid != MyProcPid) {
        log_line_number = 0;
        log_my_pid = MyProcPid;
        reset_formatted_start_time();
    }
    log_line_number++;

    initStringInfo(&csv_buffer);

    // Build CSV record with essential fields
    // Timestamp
    appendStringInfoString(&csv_buffer, get_formatted_log_time());
    appendStringInfoChar(&csv_buffer, ',');

    // User and database info
    if (MyProcPort) {
        appendCSVLiteral(&csv_buffer, MyProcPort->user_name);
        appendStringInfoChar(&csv_buffer, ',');
        appendCSVLiteral(&csv_buffer, MyProcPort->database_name);
    } else {
        appendStringInfoString(&csv_buffer, ",,");
    }
    appendStringInfoChar(&csv_buffer, ',');

    // Process identification
    appendStringInfo(&csv_buffer, "%d,", MyProcPid);

    // Remote host:port
    if (MyProcPort && MyProcPort->remote_host) {
        appendStringInfo(&csv_buffer, "\"%s", MyProcPort->remote_host);
        if (MyProcPort->remote_port && MyProcPort->remote_port[0] != '\0') {
            appendStringInfo(&csv_buffer, ":%s", MyProcPort->remote_port);
        }
        appendStringInfoChar(&csv_buffer, '"');
    }
    appendStringInfoChar(&csv_buffer, ',');

    // Session and transaction info
    appendStringInfo(&csv_buffer, "%" INT64_MODIFIER "x.%x,%ld,",
                     MyStartTime, MyProcPid, log_line_number);

    // Error information
    appendStringInfoString(&csv_buffer, _(error_severity(edata->elevel)));
    appendStringInfoChar(&csv_buffer, ',');
    appendStringInfoString(&csv_buffer, unpack_sql_state(edata->sqlerrcode));
    appendStringInfoChar(&csv_buffer, ',');

    // Message and details
    appendCSVLiteral(&csv_buffer, edata->message);
    appendStringInfoChar(&csv_buffer, ',');
    appendCSVLiteral(&csv_buffer, edata->detail_log ? edata->detail_log : edata->detail);
    appendStringInfoChar(&csv_buffer, ',');
    appendCSVLiteral(&csv_buffer, edata->hint);
    appendStringInfoChar(&csv_buffer, ',');

    // Query information (if logging enabled)
    if (check_log_of_query(edata)) {
        appendCSVLiteral(&csv_buffer, debug_query_string);
    }
    appendStringInfoChar(&csv_buffer, ',');

    // Query ID
    appendStringInfo(&csv_buffer, "%lld\n", (long long) pgstat_get_my_query_id());

    // Write to appropriate destination
    if (MyBackendType == B_LOGGER) {
        write_syslogger_file(csv_buffer.data, csv_buffer.len, LOG_DESTINATION_CSVLOG);
    } else {
        write_pipe_chunks(csv_buffer.data, csv_buffer.len, LOG_DESTINATION_CSVLOG);
    }

    pfree(csv_buffer.data);
}
```

Key simplifications made:
- Consolidated similar append operations where possible
- Removed some optional fields while keeping essential ones (timestamp, user, database, process, error info, message)
- Simplified host:port formatting logic
- Focused on the core CSV construction and output flow
- Preserved the essential structure: setup, field construction, and output
- Maintained proper memory cleanup and destination handling