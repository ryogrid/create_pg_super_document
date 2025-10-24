# network_in

## Location
[src/backend/utils/adt/network.c:75-120](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/network.c#L75-L120)

## Overview
A common input parsing function for INET and CIDR data types that converts string representations of network addresses into PostgreSQL's internal inet structure.

## Definition

```c
static inet *
network_in(char *src, bool is_cidr, Node *escontext)
```
## Detailed Description
This function serves as the core parsing routine for both INET and CIDR data types in PostgreSQL. It takes a string representation of an IP address (with optional network mask) and converts it into PostgreSQL's internal inet structure. The function automatically detects whether the input is an IPv4 or IPv6 address by checking for the presence of colons, then uses the appropriate parsing logic. For CIDR inputs, it performs additional validation to ensure no bits are set beyond the network mask.

## Parameters / Member Variables
- `*src`: String representation of the network address to parse
- `is_cidr`: Boolean flag indicating whether to enforce CIDR validation rules
- `*escontext`: Error context for soft error handling
## Dependencies
- Functions called/Symbols referenced:
  - inet (data type)
  - [palloc0](../p/palloc0.md) (memory allocation)
  - strchr (string search)
  - [pg_inet_net_pton](../p/pg_inet_net_pton.md) (network address parsing)
  - ip_family, ip_addr, ip_bits, ip_addrsize, ip_maxbits (inet accessor macros)
  - [addressOK](../a/addressOK.md) (CIDR validation)
  - SET_INET_VARSIZE (size setting macro)
  - ereturn (error return macro)
  - PGSQL_AF_INET, PGSQL_AF_INET6 (address family constants)
- Called from (representative examples):
  - [inet_in](../i/inet_in.md) (INET type input function)
  - [cidr_in](../c/cidr_in.md) (CIDR type input function)
  - [inet_client_addr](../i/inet_client_addr.md) (client address retrieval)
  - [inet_server_addr](../i/inet_server_addr.md) (server address retrieval)

## Notes and Other Information
- The function is marked as static, indicating it's an internal helper function within the network.c module
- IPv6 detection is performed by checking for the presence of ':' characters in the input string
- For CIDR types, additional validation ensures the address portion doesn't have bits set beyond the network mask
- Uses PostgreSQL's error context system for proper error handling and reporting
- The function allocates memory using palloc0 to ensure the inet structure is zero-initialized

## Simplified Source

```c
static inet *
network_in(char *src, bool is_cidr, Node *escontext)
{
    int bits;
    inet *dst;

    // Allocate and initialize inet structure
    dst = (inet *) palloc0(sizeof(inet));

    // Detect IP version: IPv6 has colons, IPv4 doesn't
    if (strchr(src, ':') != NULL)
        ip_family(dst) = PGSQL_AF_INET6;
    else
        ip_family(dst) = PGSQL_AF_INET;

    // Parse the network address string
    bits = pg_inet_net_pton(ip_family(dst), src, ip_addr(dst),
                            is_cidr ? ip_addrsize(dst) : -1);

    // Validate parsing result
    if ((bits < 0) || (bits > ip_maxbits(dst)))
        ereturn(escontext, NULL, /* invalid input syntax error */);

    // CIDR validation: ensure no bits set beyond mask
    if (is_cidr) {
        if (!addressOK(ip_addr(dst), bits, ip_family(dst)))
            ereturn(escontext, NULL, /* invalid CIDR value error */);
    }

    // Set final properties
    ip_bits(dst) = bits;
    SET_INET_VARSIZE(dst);

    return dst;
}
```