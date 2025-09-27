# write_jsonlog

## Location
[src/backend/utils/error/jsonlog.c:109-301](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/error/jsonlog.c#L109-L301)

## Overview
The main function responsible for formatting and writing PostgreSQL log messages in JSON format, converting ErrorData structures into structured JSON log entries.

## Definition

```c
void
write_jsonlog(ErrorData *edata)
```
## Detailed Description
This function takes PostgreSQL error/log data and formats it as a structured JSON log entry containing comprehensive session and error information. It serves as the primary JSON logging mechanism in PostgreSQL, producing machine-readable log output that includes timestamps, process information, session details, transaction context, error details, and query information.

The function maintains a static line counter that resets when the process ID changes, ensuring proper log line numbering across process boundaries. It constructs a JSON object by systematically adding key-value pairs for various PostgreSQL logging attributes, handling optional fields gracefully by only including them when relevant data is available.

The output is written either directly to the log file (for syslogger processes) or sent through pipes to the logging infrastructure, depending on the backend type.

## Parameters / Member Variables
- : Pointer to ErrorData structure containing the log message details, error level, SQL state, message text, context, and other error-related information

## Dependencies
- Functions called/Symbols referenced:
  -  (time formatting reset)
  -  (current timestamp formatting)
  -  (JSON string escaping)
  -  (formatted JSON key-value appending)
  -  (process display string)
  -  (binary string appending)
  -  (session start time formatting)
  -  (transaction ID retrieval)
  -  (error level to string conversion)
  -  (SQL state code formatting)
  -  (query logging policy check)
  -  (backend type identification)
  -  (query ID retrieval)
  -  (direct file writing for syslogger)
  -  (pipe-based log writing)
- Called from (representative examples):
  -  (in elog.c:3360)
  - Used via  constant

## Notes and Other Information
- Maintains process-specific static counters for log line numbering that reset on process changes
- Generates comprehensive JSON logs including session metadata, process information, transaction context, and error details
- Handles optional fields gracefully by only including them when data is available
- Supports both direct file writing (for syslogger processes) and pipe-based communication
- Implements proper memory management with cleanup of temporary buffers
- Coordinates with PostgreSQL's logging infrastructure to respect verbosity settings and query logging policies
- The JSON output format includes fields like timestamp, user, database, PID, session info, error details, SQL state, query information, and file location data
- Integrates with PostgreSQL's transaction and process management systems to provide accurate context information

## Simplified Source

```c
// Simplified version of write_jsonlog
void write_jsonlog(ErrorData *edata) {
    StringInfoData json_buffer;
    static long log_line_number = 0;
    static int log_my_pid = 0;

    // Reset line counter when process changes
    if (log_my_pid != MyProcPid) {
        log_line_number = 0;
        log_my_pid = MyProcPid;
        reset_formatted_start_time();
    }
    log_line_number++;

    initStringInfo(&json_buffer);
    appendStringInfoChar(&json_buffer, '{');

    // Timestamp (first field, no comma prefix)
    escape_json(&json_buffer, "timestamp");
    appendStringInfoChar(&json_buffer, ':');
    escape_json(&json_buffer, get_formatted_log_time());

    // Session information
    if (MyProcPort) {
        appendJSONKeyValue(&json_buffer, "user", MyProcPort->user_name, true);
        appendJSONKeyValue(&json_buffer, "dbname", MyProcPort->database_name, true);
        if (MyProcPort->remote_host) {
            appendJSONKeyValue(&json_buffer, "remote_host", MyProcPort->remote_host, true);
        }
    }

    // Process information
    if (MyProcPid != 0) {
        appendJSONKeyValueFmt(&json_buffer, "pid", false, "%d", MyProcPid);
    }
    appendJSONKeyValueFmt(&json_buffer, "session_id", true, "%" INT64_MODIFIER "x.%x",
                          MyStartTime, MyProcPid);
    appendJSONKeyValueFmt(&json_buffer, "line_num", false, "%ld", log_line_number);

    // Transaction information
    if (MyProc != NULL && MyProc->vxid.procNumber != INVALID_PROC_NUMBER) {
        appendJSONKeyValueFmt(&json_buffer, "vxid", true, "%d/%u",
                              MyProc->vxid.procNumber, MyProc->vxid.lxid);
    }
    appendJSONKeyValueFmt(&json_buffer, "txid", false, "%u", GetTopTransactionIdIfAny());

    // Error information
    if (edata->elevel) {
        appendJSONKeyValue(&json_buffer, "error_severity",
                           (char *) error_severity(edata->elevel), true);
    }
    if (edata->sqlerrcode) {
        appendJSONKeyValue(&json_buffer, "state_code",
                           unpack_sql_state(edata->sqlerrcode), true);
    }

    // Message content
    appendJSONKeyValue(&json_buffer, "message", edata->message, true);
    if (edata->detail_log || edata->detail) {
        appendJSONKeyValue(&json_buffer, "detail",
                           edata->detail_log ? edata->detail_log : edata->detail, true);
    }
    if (edata->hint) {
        appendJSONKeyValue(&json_buffer, "hint", edata->hint, true);
    }

    // Query information (if logging enabled)
    if (check_log_of_query(edata)) {
        appendJSONKeyValue(&json_buffer, "statement", debug_query_string, true);
        if (edata->cursorpos > 0) {
            appendJSONKeyValueFmt(&json_buffer, "cursor_position", false, "%d", edata->cursorpos);
        }
    }

    // Backend and query ID
    appendJSONKeyValue(&json_buffer, "backend_type", get_backend_type_for_log(), true);
    appendJSONKeyValueFmt(&json_buffer, "query_id", false, "%lld",
                          (long long) pgstat_get_my_query_id());

    // Finalize JSON
    appendStringInfoChar(&json_buffer, '}');
    appendStringInfoChar(&json_buffer, '\n');

    // Write to appropriate destination
    if (MyBackendType == B_LOGGER) {
        write_syslogger_file(json_buffer.data, json_buffer.len, LOG_DESTINATION_JSONLOG);
    } else {
        write_pipe_chunks(json_buffer.data, json_buffer.len, LOG_DESTINATION_JSONLOG);
    }

    pfree(json_buffer.data);
}
```

Key simplifications made:
- Consolidated JSON field construction into logical groups (session, process, transaction, error, message)
- Removed some optional fields while keeping essential ones
- Simplified conditional field inclusion logic
- Maintained proper JSON structure with opening/closing braces
- Preserved the line counter reset logic and output destination handling