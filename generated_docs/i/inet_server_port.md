# inet_server_port

## Location
[src/backend/utils/adt/network.c:1825-1856](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/network.c#L1825-L1856)

## Overview
Returns the port number that the PostgreSQL server accepted the current connection on, or NULL if the connection is through a Unix socket.

## Definition
```c
Datum inet_server_port(PG_FUNCTION_ARGS)
```

## Detailed Description
This function retrieves the local port number of the server socket that accepted the current client connection. It accesses the connection information stored in MyProcPort and extracts the local address (laddr) from the port structure. The function supports both IPv4 (AF_INET) and IPv6 (AF_INET6) address families and returns NULL for Unix domain sockets or when port resolution fails.

The function uses pg_getnameinfo_all() to convert the binary socket address to a string representation in numeric format, focusing on the port component. The resulting port string is then converted to a PostgreSQL int4 (32-bit integer) type using the int4in function through DirectFunctionCall1.

## Parameters / Member Variables
This function takes no explicit parameters (uses PG_FUNCTION_ARGS macro for PostgreSQL function interface).

## Dependencies
- Functions called/Symbols referenced:
  - [Port](../P/Port.md) (struct type from MyProcPort)
  - [pg_getnameinfo_all](../p/pg_getnameinfo_all.md) (address-to-string conversion)
  - [int4in](int4in.md) (string to int4 conversion function)
  - DirectFunctionCall1 (PostgreSQL function call interface)
  - [CStringGetDatum](../C/CStringGetDatum.md) (C string to Datum conversion)
  - PG_RETURN_DATUM (PostgreSQL return macro)
- Called from (representative examples):
  - No direct callers found (likely called through SQL function interface)

## Notes and Other Information
- Returns NULL for Unix socket connections (non-TCP connections)
- Only supports IPv4 and IPv6 address families
- Uses NI_NUMERICHOST | NI_NUMERICSERV flags to ensure numeric output format
- Returns the port as an integer value, not as text
- Part of PostgreSQL's network data type functions
- Accessible from SQL as inet_server_port() function
- Relies on MyProcPort global variable which contains current connection information

## Simplified Source

```c
Datum
inet_server_port(PG_FUNCTION_ARGS)
{
    Port *port = MyProcPort;
    char local_port[NI_MAXSERV];

    // Return NULL if no port info or not TCP/IP connection
    if (port == NULL)
        PG_RETURN_NULL();

    // Only handle IPv4 and IPv6 connections
    if (port->laddr.addr.ss_family != AF_INET &&
        port->laddr.addr.ss_family != AF_INET6)
        PG_RETURN_NULL();

    // Extract server port number from local socket address
    if (pg_getnameinfo_all(&port->laddr.addr, port->laddr.salen,
                          NULL, 0, local_port, sizeof(local_port),
                          NI_NUMERICHOST | NI_NUMERICSERV) != 0)
        PG_RETURN_NULL();

    // Convert port string to PostgreSQL integer
    PG_RETURN_DATUM(DirectFunctionCall1(int4in, CStringGetDatum(local_port)));
}
```