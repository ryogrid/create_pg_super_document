# pqParseIntParam

## Location
[src/interfaces/libpq/fe-connect.c:7694-7744](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-connect.c#L7694-L7744)

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
  - [libpq_append_conn_error](../l/libpq_append_conn_error.md) (PostgreSQL error reporting function)
  - Assert (PostgreSQL assertion macro)
- Called from (representative examples):
  - [PQgetCancel](../P/PQgetCancel.md) (multiple calls for parsing port and timeout values)
  - [useKeepalives](../u/useKeepalives.md), setKeepalivesIdle, setKeepalivesInterval, setKeepalivesCount
  - [setTCPUserTimeout](../s/setTCPUserTimeout.md)
  - [pqConnectDBComplete](pqConnectDBComplete.md), PQconnectPoll

## Notes and Other Information
- Returns true on successful parsing, false on error
- Initializes result to 0 before parsing
- Skips leading whitespace automatically via strtol()
- Manually skips trailing whitespace and ensures string ends properly
- Detects overflow by checking if parsed long value fits in int range
- Sets errno to 0 before parsing to detect strtol() errors
- Provides descriptive error messages including the invalid value and parameter name
- Used extensively throughout libpq for parsing integer connection options

## Simplified Source

```c
bool
pqParseIntParam(const char *value, int *result, PGconn *conn, const char *context)
{
    Assert(value != NULL);
    *result = 0;

    // Parse string to long, skipping leading whitespace
    errno = 0;
    char *end;
    long numval = strtol(value, &end, 10);

    // Check for parsing errors or overflow
    if (value == end || errno != 0 || numval != (int) numval) {
        goto error; // No progress, error, or overflow
    }

    // Skip trailing whitespace
    while (*end != '\0' && isspace((unsigned char) *end)) {
        end++;
    }

    // Ensure string ends properly (no trailing garbage)
    if (*end != '\0') {
        goto error;
    }

    // Success: store result and return
    *result = numval;
    return true;

error:
    libpq_append_conn_error(conn,
        "invalid integer value \"%s\" for connection option \"%s\"",
        value, context);
    return false;
}
```