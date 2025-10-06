# network_host

## Location
[src/backend/utils/adt/network.c:1173-1198](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/network.c#L1173-L1198)

## Overview
Extracts the host portion from a network datatype (inet/cidr), returning it as a text representation without the netmask component.

## Definition

```c
Datum
network_host(PG_FUNCTION_ARGS)
```
## Detailed Description
The network_host function is a PostgreSQL built-in function that takes an inet or cidr value and returns only the host address portion as text. It strips away any netmask information that may be present in the original value. The function uses pg_inet_net_ntop to format the address with maximum bit precision, then removes any trailing netmask notation (e.g., "/24") if present. This is useful when you need just the IP address without network information.

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
  - [cstring_to_text](../c/cstring_to_text.md) (to convert C string to PostgreSQL text)
  - PG_RETURN_TEXT_P (to return text result)
- Called from (representative examples):
  - No direct references found in the analyzed codebase

## Notes and Other Information
- Located in src/backend/utils/adt/network.c:1173-1198
- Forces display of maximum bits regardless of the original masklen
- Suppresses any trailing netmask notation by truncating at the '/' character
- Returns an error if the inet value cannot be formatted properly
- Uses a temporary buffer sized to handle the longest possible IPv6 address representation

## Simplified Source

```c
Datum
network_host(PG_FUNCTION_ARGS)
{
    inet       *ip = PG_GETARG_INET_PP(0);
    char       *ptr;
    char        tmp[sizeof("xxxx:xxxx:xxxx:xxxx:xxxx:xxxx:255.255.255.255/128")];

    // Format the network address with maximum precision
    if (pg_inet_net_ntop(ip_family(ip), ip_addr(ip), ip_maxbits(ip),
                         tmp, sizeof(tmp)) == NULL)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_BINARY_REPRESENTATION),
                 errmsg("could not format inet value: %m")));

    // Remove netmask portion if present (strip "/n")
    if ((ptr = strchr(tmp, '/')) != NULL)
        *ptr = '\0';

    PG_RETURN_TEXT_P(cstring_to_text(tmp));
}
```