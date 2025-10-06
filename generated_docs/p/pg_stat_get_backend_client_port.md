# pg_stat_get_backend_client_port

## Location
[src/backend/utils/adt/pgstatfuncs.c:924-969](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/pgstatfuncs.c#L924-L969)

## Overview
Returns the client port number of a backend process identified by its process number, accessible through PostgreSQL's statistics views.

## Definition
```c
Datum pg_stat_get_backend_client_port(PG_FUNCTION_ARGS)
```

## Detailed Description
This function retrieves the client port number for a specific backend process in PostgreSQL. It takes a backend process number as input and returns the port number of the client connected to that backend. The function performs several validation checks similar to its address counterpart:

1. Validates that the backend entry exists
2. Checks user permissions to access the statistics information
3. Verifies that client address information is available (not zeroed)
4. Supports IPv4, IPv6, and Unix socket connections
5. For Unix sockets (AF_UNIX), returns -1 as a special indicator
6. Converts the internal socket port to an integer value

The function returns NULL if the backend doesn't exist, the user lacks permissions, no client address is available, or if port resolution fails.

## Parameters / Member Variables
- `procNumber` (int32): The backend process number to query for client port information

## Dependencies
- Functions called/Symbols referenced:
  - [pgstat_get_beentry_by_proc_number](pgstat_get_beentry_by_proc_number.md)
  - HAS_PGSTAT_PERMISSIONS
  - [pg_getnameinfo_all](pg_getnameinfo_all.md)
  - [int4in](../i/int4in.md)
  - DirectFunctionCall1
  - [CStringGetDatum](../C/CStringGetDatum.md)
- Data types used:
  - [PgBackendStatus](../P/PgBackendStatus.md)
  - [SockAddr](../S/SockAddr.md)

## Notes and Other Information
- The function is used internally by PostgreSQL's statistics system functions
- Returns NULL when client port is unavailable or access is denied
- Handles IPv4 (AF_INET), IPv6 (AF_INET6), and Unix socket (AF_UNIX) connections
- Unix socket connections return -1 since they don't have meaningful port numbers
- Used by system views like pg_stat_activity to display client connection information
- Access is subject to PostgreSQL's statistics permissions model
- Complements pg_stat_get_backend_client_addr for complete client connection information

## Simplified Source

```c
Datum
pg_stat_get_backend_client_port(PG_FUNCTION_ARGS)
{
    int32 procNumber = PG_GETARG_INT32(0);
    PgBackendStatus *beentry;
    SockAddr zero_clientaddr;
    char remote_port[NI_MAXSERV];
    int ret;

    // Get backend entry for the process number
    if ((beentry = pgstat_get_beentry_by_proc_number(procNumber)) == NULL)
        PG_RETURN_NULL();

    // Check permissions to access statistics
    if (!HAS_PGSTAT_PERMISSIONS(beentry->st_userid))
        PG_RETURN_NULL();

    // Check if client address is available (not zero)
    memset(&zero_clientaddr, 0, sizeof(zero_clientaddr));
    if (memcmp(&(beentry->st_clientaddr), &zero_clientaddr, sizeof(zero_clientaddr)) == 0)
        PG_RETURN_NULL();

    // Handle different address families
    switch (beentry->st_clientaddr.addr.ss_family) {
        case AF_INET:
        case AF_INET6:
            break;
        case AF_UNIX:
            PG_RETURN_INT32(-1);  // Unix sockets don't have ports
        default:
            PG_RETURN_NULL();
    }

    // Extract port number from socket address
    remote_port[0] = '\0';
    ret = pg_getnameinfo_all(&beentry->st_clientaddr.addr,
                            beentry->st_clientaddr.salen,
                            NULL, 0,
                            remote_port, sizeof(remote_port),
                            NI_NUMERICHOST | NI_NUMERICSERV);
    if (ret != 0)
        PG_RETURN_NULL();

    // Convert port string to integer and return
    PG_RETURN_DATUM(DirectFunctionCall1(int4in, CStringGetDatum(remote_port)));
}
```