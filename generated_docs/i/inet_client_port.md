# inet_client_port

## Location
[src/backend/utils/adt/network.c:1753-1787](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/network.c#L1753-L1787)

## Overview
Returns the port number that the client is connecting from, or NULL if the connection is via Unix socket or if port resolution fails.

## Definition

```c
Datum
inet_client_port(PG_FUNCTION_ARGS)
```
## Detailed Description
This function provides the source port number of the client that established the current database connection. It's a PostgreSQL built-in function that can be called from SQL to determine the network port used by the connecting client.

The function works by:
1. Accessing the current process's port information through MyProcPort
2. Checking if the connection uses a supported address family (AF_INET for IPv4 or AF_INET6 for IPv6)
3. Converting the socket address to extract the port number using pg_getnameinfo_all
4. Converting the port string to PostgreSQL's integer format

The function returns NULL in several cases:
- No port information available (server-side processes without client connections)
- Unix socket connections (local connections not using TCP/IP)
- Unsupported address families
- Port resolution failures

This information is particularly useful for logging, auditing, and debugging network connections, as it provides the complete client endpoint information when combined with inet_client_addr().

## Parameters / Member Variables
This function takes no parameters (uses PG_FUNCTION_ARGS macro for PostgreSQL function interface)

## Dependencies
- Functions called/Symbols referenced:
  - [Port](../P/Port.md) (structure representing connection port information)
  - MyProcPort (global variable pointing to current process's port)
  - [pg_getnameinfo_all](../p/pg_getnameinfo_all.md) (PostgreSQL wrapper for address-to-name conversion)
  - [int4in](int4in.md) (function to parse integer from string)
  - DirectFunctionCall1 (PostgreSQL function call interface)
  - [CStringGetDatum](../C/CStringGetDatum.md) (converts C string to Datum type)
  - PG_RETURN_DATUM (macro to return generic Datum value)
  - PG_RETURN_NULL (macro to return SQL NULL)

- Called from (representative examples):
  - Available as SQL function inet_client_port() callable from queries

## Notes and Other Information
- This function is accessible from SQL as a built-in function
- Returns NULL for local Unix socket connections, matching the behavior of inet_client_addr
- The NI_NUMERICSERV flag ensures numeric port numbers are returned
- Client port numbers are typically ephemeral ports assigned by the client's operating system
- Essential for complete connection tracking and network forensics
- Part of PostgreSQL's connection information functions for network-based connections
- The returned port number is always numeric, never a service name

## Simplified Source

```c
Datum
inet_client_port(PG_FUNCTION_ARGS)
{
    Port *port = MyProcPort;
    char remote_port[NI_MAXSERV];

    // Return NULL if no port info or not TCP/IP connection
    if (port == NULL)
        PG_RETURN_NULL();

    // Only handle IPv4 and IPv6 connections
    if (port->raddr.addr.ss_family != AF_INET &&
        port->raddr.addr.ss_family != AF_INET6)
        PG_RETURN_NULL();

    // Extract port number from socket address
    if (pg_getnameinfo_all(&port->raddr.addr, port->raddr.salen,
                          NULL, 0, remote_port, sizeof(remote_port),
                          NI_NUMERICHOST | NI_NUMERICSERV) != 0)
        PG_RETURN_NULL();

    // Convert port string to PostgreSQL integer
    PG_RETURN_DATUM(DirectFunctionCall1(int4in, CStringGetDatum(remote_port)));
}
```