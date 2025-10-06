# network_show

## Location
[src/backend/utils/adt/network.c:1199-1221](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/network.c#L1199-L1221)

## Overview
Implements the inet and cidr casts to text, providing a complete network representation including both the address and netmask components.

## Definition

```c
Datum
network_show(PG_FUNCTION_ARGS)
```
## Detailed Description
The network_show function converts inet or cidr values to their text representation, always including the netmask portion. Unlike network_out, this function has specialized behavior for casting operations and ensures that the netmask is always displayed (e.g., "192.168.1.1/24"). If the formatted address doesn't already include a netmask (which it typically won't when using pg_inet_net_ntop with maximum bits), the function appends the actual netmask length from the inet value. This provides a complete network specification in text format.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  - Argument 0: inet/cidr value (accessed via PG_GETARG_INET_PP(0))

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INET_PP (to extract inet argument)
  - ip_family (to get address family)
  - ip_addr (to get address data)
  - ip_maxbits (to get maximum bits for the address type)
  - [pg_inet_net_ntop](../p/pg_inet_net_ntop.md) (to format the network address)
  - ip_bits (to get the actual netmask length)
  - [cstring_to_text](../c/cstring_to_text.md) (to convert C string to PostgreSQL text)
  - PG_RETURN_TEXT_P (to return text result)
- Called from (representative examples):
  - No direct references found in the analyzed codebase

## Notes and Other Information
- Located in src/backend/utils/adt/network.c:1199-1221
- Specifically designed for inet and cidr casts to text, with behavior distinct from network_out
- Always ensures the netmask component is included in the output
- Uses pg_inet_net_ntop with maximum bits, then adds the actual netmask if not present
- Cannot be replaced with CoerceViaIO due to its specialized casting behavior
- Uses a temporary buffer sized to handle the longest possible IPv6 address with netmask representation

## Simplified Source

```c
Datum
network_show(PG_FUNCTION_ARGS)
{
    inet       *ip = PG_GETARG_INET_PP(0);
    int         len;
    char        tmp[sizeof("xxxx:xxxx:xxxx:xxxx:xxxx:xxxx:255.255.255.255/128")];

    // Format the network address with maximum precision
    if (pg_inet_net_ntop(ip_family(ip), ip_addr(ip), ip_maxbits(ip),
                         tmp, sizeof(tmp)) == NULL)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_BINARY_REPRESENTATION),
                 errmsg("could not format inet value: %m")));

    // Add netmask if not present (which it won't be)
    if (strchr(tmp, '/') == NULL)
    {
        len = strlen(tmp);
        snprintf(tmp + len, sizeof(tmp) - len, "/%u", ip_bits(ip));
    }

    PG_RETURN_TEXT_P(cstring_to_text(tmp));
}
```