# write_jsonlog

## Location
[src/backend/utils/error/jsonlog.c:109-301](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/error/jsonlog.c#L109-L301)

## Overview
The main function responsible for formatting and writing PostgreSQL log messages in JSON format, converting ErrorData structures into structured JSON log entries.

## Definition


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