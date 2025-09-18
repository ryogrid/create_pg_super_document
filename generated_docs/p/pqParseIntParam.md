# pqParseIntParam

## Location
src/interfaces/libpq/fe-connect.c: 7694 - 7744

## Overview
Parses and validates a string value as an integer for PostgreSQL connection parameters with proper error handling and range checking.

## Definition
```c
bool pqParseIntParam(const char *value, int *result, PGconn *conn, const char *context)
```

## Detailed Description
This function safely converts a string representation to an integer value with comprehensive validation. It handles leading and trailing whitespace, detects parsing errors, checks for overflow conditions, and ensures no trailing garbage characters remain. The function provides detailed error messages through the connection object when parsing fails, making it suitable for validating user-provided connection parameters.

## Parameters / Member Variables
- `value`: String to parse as an integer (must not be NULL)
- `result`: Pointer to integer where the parsed value will be stored
- `conn`: PostgreSQL connection object for error reporting
- `context`: String describing the connection option name for error messages

## Dependencies
- Functions called/Symbols referenced:
  - strtol (standard C library function for string to long conversion)
  - isspace (standard C library function for whitespace checking)
  - libpq_append_conn_error (PostgreSQL error reporting function)
  - Assert (PostgreSQL assertion macro)
- Called from (representative examples):
  - PQgetCancel (multiple calls for parsing port and timeout values)
  - useKeepalives, setKeepalivesIdle, setKeepalivesInterval, setKeepalivesCount
  - setTCPUserTimeout
  - pqConnectDBComplete, PQconnectPoll

## Notes and Other Information
- Returns true on successful parsing, false on error
- Initializes result to 0 before parsing
- Skips leading whitespace automatically via strtol()
- Manually skips trailing whitespace and ensures string ends properly
- Detects overflow by checking if parsed long value fits in int range
- Sets errno to 0 before parsing to detect strtol() errors
- Provides descriptive error messages including the invalid value and parameter name
- Used extensively throughout libpq for parsing integer connection options