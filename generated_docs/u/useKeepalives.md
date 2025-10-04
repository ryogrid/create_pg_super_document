# useKeepalives

## Location
[src/interfaces/libpq/fe-connect.c:2169-2186](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-connect.c#L2169-L2186)

## Overview
Determines whether TCP keepalive should be enabled for a connection by parsing the keepalives connection parameter and returning a standardized result code.

## Definition
```c
static int useKeepalives(PGconn *conn)
```

## Detailed Description
This function evaluates the `keepalives` connection parameter to determine if TCP keepalive functionality should be enabled. It handles three scenarios:

1. **NULL parameter**: Returns 1 (enable keepalives by default)
2. **Valid integer parameter**: Parses the value and returns 1 if non-zero, 0 if zero
3. **Invalid parameter**: Returns -1 to indicate a parsing error

The function uses libpq's standard parameter parsing mechanism to validate and convert the string parameter to an integer value, ensuring consistent error handling across the connection establishment process.

## Parameters / Member Variables
- `conn`: Pointer to the PGconn connection object containing the keepalives parameter string

## Return Value
- **1**: Enable TCP keepalives (default behavior or non-zero parameter value)
- **0**: Disable TCP keepalives (parameter explicitly set to zero)  
- **-1**: Error in parameter parsing (invalid non-integer value)

## Dependencies
- Functions called/Symbols referenced:
  - [pqParseIntParam](../p/pqParseIntParam.md) (for parsing integer connection parameters)
- Called from (representative examples):
  - CONNECTION_FAILED (during connection setup and configuration)

## Notes and Other Information
- This is a static function internal to fe-connect.c
- Part of libpq's TCP keepalive configuration system
- Default behavior (when keepalives=NULL) is to enable keepalives
- Uses libpq's standard error reporting through the connection object
- The parsed value follows standard C semantics: any non-zero value means "enable"

## Simplified Source

```c
static int useKeepalives(PGconn *conn) {
    int val;

    // Default to enabled if not specified
    if (conn->keepalives == NULL)
        return 1;

    // Parse the parameter value
    if (!pqParseIntParam(conn->keepalives, &val, conn, "keepalives"))
        return -1;  // Parse error

    // Return 1 for enabled (non-zero), 0 for disabled (zero)
    return val != 0 ? 1 : 0;
}
```