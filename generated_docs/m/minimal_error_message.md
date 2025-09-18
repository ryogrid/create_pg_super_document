# minimal_error_message

## Location
[src/bin/psql/command.c:5956-5978](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/command.c#L5956-L5978)

## Overview
Reports only the primary error information from a PostgreSQL result to avoid cluttering output with verbose error details and internally generated queries.

## Definition
```c
static void minimal_error_message(PGresult *res)
```

## Detailed Description
This function extracts and displays essential error information from a PostgreSQL query result, providing a clean, minimal error report. It formats the error message by combining the severity level (if available) with the primary error message, avoiding the display of additional diagnostic information that might confuse users. The function creates a formatted error message that includes the error severity (or defaults to "ERROR:" if not available) followed by the primary error message text.

The function is designed to provide user-friendly error reporting by filtering out verbose diagnostic information and internal query details that are typically not useful for end users, particularly in interactive psql sessions.

## Parameters / Member Variables
- `res`: A PGresult structure containing the result of a PostgreSQL operation, including any error information that occurred during the operation

## Dependencies
- Functions called/Symbols referenced:
  - createPQExpBuffer (PostgreSQL utility function)
  - [PQresultErrorField](../P/PQresultErrorField.md) (libpq function)
  - [printfPQExpBuffer](../p/printfPQExpBuffer.md) (PostgreSQL utility function)  
  - [appendPQExpBufferStr](../a/appendPQExpBufferStr.md) (PostgreSQL utility function)
  - appendPQExpBufferChar (PostgreSQL utility function)
  - pg_log_error (PostgreSQL logging function)
  - destroyPQExpBuffer (PostgreSQL utility function)
  - PG_DIAG_SEVERITY (PostgreSQL diagnostic field constant)
  - PG_DIAG_MESSAGE_PRIMARY (PostgreSQL diagnostic field constant)
- Called from (representative examples):
  - [lookup_object_oid](../l/lookup_object_oid.md) (in src/bin/psql/command.c:5651)
  - [get_create_object_cmd](../g/get_create_object_cmd.md) (in src/bin/psql/command.c:5806)

## Notes and Other Information
- Uses PQExpBuffer for safe string construction and memory management
- Falls back to "ERROR:" if no severity information is available in the result
- Falls back to "(not available)" if no primary message is available
- Automatically appends a newline character to the formatted message
- Memory management is handled properly with createPQExpBuffer/destroyPQExpBuffer pairing
- Specifically designed to reduce noise in error reporting by excluding secondary diagnostic information
- Commonly used in psql command implementations where detailed error context is not needed