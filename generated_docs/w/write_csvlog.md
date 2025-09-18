# write_csvlog

## Location
[src/backend/utils/error/csvlog.c:63-262](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/error/csvlog.c#L63-L262)

## Overview
Generates and writes a comprehensive CSV-formatted log entry containing error/message data and process information according to PostgreSQL's structured logging format.

## Definition


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
  - get_ps_display (gets process status display string)
  - [GetTopTransactionIdIfAny](../G/GetTopTransactionIdIfAny.md) (gets current transaction ID)
  - [error_severity](../e/error_severity.md) (converts error level to severity string)
  - [unpack_sql_state](../u/unpack_sql_state.md) (converts SQL error code to state string)
  - [check_log_of_query](../c/check_log_of_query.md) (determines if query should be logged)
  - [get_backend_type_for_log](../g/get_backend_type_for_log.md) (gets backend type string)
  - pgstat_get_my_query_id (gets current query ID)
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