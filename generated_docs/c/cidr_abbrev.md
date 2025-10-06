# cidr_abbrev

## Location
[src/backend/utils/adt/network.c:1240-1257](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/network.c#L1240-L1257)

## Overview
Converts a cidr value to its abbreviated text representation using CIDR-specific formatting that zeroes out host bits.

## Definition

```c
Datum
cidr_abbrev(PG_FUNCTION_ARGS)
```
## Detailed Description
The cidr_abbrev function provides an abbreviated representation of a cidr value using pg_inet_cidr_ntop, which specifically formats CIDR network addresses. The key difference from inet_abbrev is that this function uses the CIDR-specific formatter that ensures host bits are properly zeroed out according to CIDR semantics. This means that any host bits beyond the network portion are set to zero in the output, providing a canonical network address representation. The function formats the address with the specified netmask length and returns it as text.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  - Argument 0: cidr value (accessed via PG_GETARG_INET_PP(0))

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INET_PP (to extract inet/cidr argument)
  - [pg_inet_cidr_ntop](../p/pg_inet_cidr_ntop.md) (to format the CIDR address with proper host bit zeroing)
  - ip_family (to get address family)
  - ip_addr (to get address data)
  - ip_bits (to get the netmask length)
  - [cstring_to_text](cstring_to_text.md) (to convert C string to PostgreSQL text)
  - PG_RETURN_TEXT_P (to return text result)
- Called from (representative examples):
  - No direct references found in the analyzed codebase

## Notes and Other Information
- Located in src/backend/utils/adt/network.c:1240-1257
- Uses pg_inet_cidr_ntop instead of pg_inet_net_ntop for CIDR-specific formatting
- Ensures host bits are properly zeroed according to CIDR standards
- Provides canonical network address representation for CIDR values
- The CIDR formatting guarantees that the output represents a proper network address, not a host address
- Error message specifically mentions "cidr value" rather than "inet value"
- Uses a temporary buffer sized to handle the longest possible IPv6 address representation

## Simplified Source

```c
Datum
cidr_abbrev(PG_FUNCTION_ARGS)
{
    inet       *ip = PG_GETARG_INET_PP(0);
    char       *dst;
    char        tmp[sizeof("xxxx:xxxx:xxxx:xxxx:xxxx:xxxx:255.255.255.255/128")];

    // Format using CIDR-specific formatting (zeros host bits)
    dst = pg_inet_cidr_ntop(ip_family(ip), ip_addr(ip),
                            ip_bits(ip), tmp, sizeof(tmp));

    if (dst == NULL)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_BINARY_REPRESENTATION),
                 errmsg("could not format cidr value: %m")));

    PG_RETURN_TEXT_P(cstring_to_text(tmp));
}
```