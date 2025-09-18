# emitHostIdentityInfo

## Location
[src/interfaces/libpq/fe-connect.c:2093-2148](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-connect.c#L2093-L2148)

## Overview
Speculatively appends connection failure context information to the connection's error message buffer to ensure subsequent error messages are properly attributed to the specific server connection target.

## Definition
```c
static void emitHostIdentityInfo(PGconn *conn, const char *host_addr)
```

## Detailed Description
This function prepares contextual error message text by appending server identification information to `conn->errorMessage`. It handles both Unix domain socket connections and TCP/IP connections differently:

- For Unix domain sockets (AF_UNIX): Formats the message to include the socket path
- For TCP/IP connections: Formats the message to include host/IP address and port information

The function intelligently displays the most appropriate host identifier, choosing between the original hostname and resolved IP address based on the connection configuration and whether they differ. This ensures error messages provide clear context about which specific server the connection attempt was targeting.

## Parameters / Member Variables
- `conn`: Pointer to the PGconn connection object containing connection state and error message buffer
- `host_addr`: String containing the resolved IP address of the connection target (result of getHostaddr())

## Dependencies
- Functions called/Symbols referenced:
  - pg_getnameinfo_all (for socket path resolution)
  - [libpq_gettext](../l/libpq_gettext.md) (for internationalized error message formatting)
  - CHT_HOST_ADDRESS (connection host type constant)
- Called from (representative examples):
  - CONNECTION_FAILED (connection failure handling state)

## Notes and Other Information
- This is a static function internal to fe-connect.c
- Requires `conn->raddr` to be valid before calling
- The function only prepares the error message prefix; actual error details are appended later
- Handles display logic for cases where hostname differs from resolved IP address
- Part of libpq's connection establishment error reporting mechanism