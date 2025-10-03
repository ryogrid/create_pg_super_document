# network_out

## Location
[src/backend/utils/adt/network.c:141-164](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/network.c#L141-L164)

## Overview
A common output formatting function for INET and CIDR data types that converts PostgreSQL's internal inet structure into string representations.

## Definition

```c
static char *
network_out(inet *src, bool is_cidr)
```
## Detailed Description
This function serves as the core formatting routine for both INET and CIDR data types in PostgreSQL. It takes an internal inet structure and converts it to a human-readable string representation. The function uses pg_inet_net_ntop to perform the actual address-to-string conversion, handling both IPv4 and IPv6 formats automatically. For CIDR outputs, it ensures that the network mask notation (/n) is always present, adding it if not already included by the underlying conversion function.

## Parameters / Member Variables
- `*src`: Pointer to the internal inet structure to be formatted
- `is_cidr`: Boolean flag indicating whether to enforce CIDR output formatting (ensure /n suffix)
## Dependencies
- Functions called/Symbols referenced:
  - inet (data type)
  - [pg_inet_net_ntop](../p/pg_inet_net_ntop.md) (network address to string conversion)
  - ip_family, ip_addr, ip_bits (inet accessor macros)
  - strchr (string search)
  - strlen (string length)
  - snprintf (formatted string printing)
  - [pstrdup](../p/pstrdup.md) (PostgreSQL string duplication)
  - ereport (error reporting)
- Called from (representative examples):
  - [inet_out](../i/inet_out.md) (INET type output function)
  - [cidr_out](../c/cidr_out.md) (CIDR type output function)

## Notes and Other Information
- The function is marked as static, indicating it's an internal helper function within the network.c module
- Uses a large temporary buffer to accommodate the longest possible IPv6 address with mask notation
- For CIDR formatting, automatically appends the mask length if not present in the converted string
- Handles both IPv4 and IPv6 addresses through the underlying pg_inet_net_ntop function
- Returns a PostgreSQL-allocated string using pstrdup for proper memory management
- Provides comprehensive error reporting if the address-to-string conversion fails
- The temporary buffer size accommodates maximum IPv6 format plus mask notation