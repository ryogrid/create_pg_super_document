# prepKeepalivesWin32

## Location
[src/interfaces/libpq/fe-connect.c:2323-2352](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-connect.c#L2323-L2352)

## Overview
Prepares and configures TCP keepalive parameters for Windows systems by parsing connection parameters and applying them via the Windows-specific keepalive interface.

## Definition

```c
static int
prepKeepalivesWin32(PGconn *conn)
```
## Detailed Description
This function serves as a preparation and coordination layer for configuring TCP keepalives on Windows systems. It extracts keepalive configuration parameters from the PostgreSQL connection structure, parses and validates them, and then delegates the actual socket configuration to the pqSetKeepalivesWin32 function. 

The function handles two key keepalive parameters: idle time (how long to wait before sending the first keepalive probe) and interval time (time between subsequent probes). It uses the pqParseIntParam utility to safely convert string parameters to integers, providing proper error handling and connection error reporting if parsing fails or if the underlying Windows socket configuration fails.

## Parameters / Member Variables
- `*conn`: Pointer to the PGconn structure representing the PostgreSQL connection. The function accesses the , , and  fields from this structure.
## Dependencies
- Functions called/Symbols referenced:
  - [pqParseIntParam](pqParseIntParam.md) (parses integer parameters from connection strings)
  - [pqSetKeepalivesWin32](pqSetKeepalivesWin32.md) (applies keepalive settings to Windows socket)
  - [libpq_append_conn_error](../l/libpq_append_conn_error.md) (appends error messages to connection)
  - WSAGetLastError (retrieves Windows socket error codes)
- Called from (representative examples):
  - CONNECTION_FAILED (connection establishment process)

## Notes and Other Information
- Windows-specific function, conditionally compiled for Windows platforms only
- Returns 1 on success, 0 on failure with appropriate error messages
- Initializes idle and interval parameters to -1, allowing pqSetKeepalivesWin32 to apply default values when parameters are not specified
- Part of the PostgreSQL libpq connection establishment process
- Provides a clean separation between parameter parsing/validation and actual socket configuration
- Handles both cases where keepalive parameters are explicitly set or left to system defaults
- Error messages include specific Windows API function names and error codes for debugging