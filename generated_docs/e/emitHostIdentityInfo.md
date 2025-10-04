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
  - [pg_getnameinfo_all](../p/pg_getnameinfo_all.md) (for socket path resolution)
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

## Simplified Source

```c
static void emitHostIdentityInfo(PGconn *conn, const char *host_addr)
{
    if (conn->raddr.addr.ss_family == AF_UNIX) {
        // Unix domain socket connection
        char service[NI_MAXHOST];

        pg_getnameinfo_all(&conn->raddr.addr, conn->raddr.salen,
                          NULL, 0, service, sizeof(service), NI_NUMERICSERV);

        appendPQExpBuffer(&conn->errorMessage,
                         libpq_gettext("connection to server on socket \"%s\" failed: "),
                         service);
    } else {
        // TCP/IP connection
        const char *displayed_host;
        const char *displayed_port;

        // Determine what host and port to display
        if (conn->connhost[conn->whichhost].type == CHT_HOST_ADDRESS) {
            displayed_host = conn->connhost[conn->whichhost].hostaddr;
        } else {
            displayed_host = conn->connhost[conn->whichhost].host;
        }

        displayed_port = conn->connhost[conn->whichhost].port;
        if (displayed_port == NULL || displayed_port[0] == '\0') {
            displayed_port = DEF_PGPORT_STR;
        }

        // Include resolved IP if different from hostname
        if (conn->connhost[conn->whichhost].type != CHT_HOST_ADDRESS &&
            host_addr[0] && strcmp(displayed_host, host_addr) != 0) {
            appendPQExpBuffer(&conn->errorMessage,
                             libpq_gettext("connection to server at \"%s\" (%s), port %s failed: "),
                             displayed_host, host_addr, displayed_port);
        } else {
            appendPQExpBuffer(&conn->errorMessage,
                             libpq_gettext("connection to server at \"%s\", port %s failed: "),
                             displayed_host, displayed_port);
        }
    }
}
```