# pg_stat_get_backend_client_addr

## Location
[src/backend/utils/adt/pgstatfuncs.c:879-923](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/pgstatfuncs.c#L879-L923)

## Overview
Returns the client IP address of a backend process identified by its process number, accessible through PostgreSQL's statistics views.

## Definition

```c
Datum
pg_stat_get_backend_client_addr(PG_FUNCTION_ARGS)
```
## Detailed Description
This function retrieves the client IP address for a specific backend process in PostgreSQL. It takes a backend process number as input and returns the IP address of the client connected to that backend. The function performs several validation checks:

1. Validates that the backend entry exists
2. Checks user permissions to access the statistics information
3. Verifies that client address information is available (not zeroed)
4. Supports both IPv4 and IPv6 addresses
5. Converts the internal socket address to a human-readable IP address string

The function returns NULL if the backend doesn't exist, the user lacks permissions, no client address is available, or if address resolution fails.

## Parameters / Member Variables
-  (int32): The backend process number to query for client address information

## Dependencies
- Functions called/Symbols referenced:
  - [pgstat_get_beentry_by_proc_number](pgstat_get_beentry_by_proc_number.md)
  - HAS_PGSTAT_PERMISSIONS
  - [pg_getnameinfo_all](pg_getnameinfo_all.md)
  - [clean_ipv6_addr](../c/clean_ipv6_addr.md)
  - [inet_in](../i/inet_in.md)
  - DirectFunctionCall1
  - [CStringGetDatum](../C/CStringGetDatum.md)
- Data types used:
  - [PgBackendStatus](../P/PgBackendStatus.md)
  - [SockAddr](../S/SockAddr.md)

## Notes and Other Information
- The function is used internally by PostgreSQL's statistics system functions
- Returns NULL when client address is unavailable or access is denied
- Handles both IPv4 (AF_INET) and IPv6 (AF_INET6) address families
- Performs IPv6 address cleaning to ensure proper formatting
- Used by system views like pg_stat_activity to display client connection information
- Access is subject to PostgreSQL's statistics permissions model

## Simplified Source

```c
Datum
pg_stat_get_backend_client_addr(PG_FUNCTION_ARGS)
{
    int32 procNumber = PG_GETARG_INT32(0);
    PgBackendStatus *beentry;
    SockAddr zero_clientaddr;
    char remote_host[NI_MAXHOST];
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

    // Verify address family is supported (IPv4 or IPv6)
    switch (beentry->st_clientaddr.addr.ss_family) {
        case AF_INET:
        case AF_INET6:
            break;
        default:
            PG_RETURN_NULL();
    }

    // Convert socket address to string representation
    remote_host[0] = '\0';
    ret = pg_getnameinfo_all(&beentry->st_clientaddr.addr,
                            beentry->st_clientaddr.salen,
                            remote_host, sizeof(remote_host),
                            NULL, 0,
                            NI_NUMERICHOST | NI_NUMERICSERV);
    if (ret != 0)
        PG_RETURN_NULL();

    // Clean IPv6 address format and return as inet datum
    clean_ipv6_addr(beentry->st_clientaddr.addr.ss_family, remote_host);
    PG_RETURN_DATUM(DirectFunctionCall1(inet_in, CStringGetDatum(remote_host)));
}
```