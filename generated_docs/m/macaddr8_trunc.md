# macaddr8_trunc

## Location
[src/backend/utils/adt/mac8.c:477-499](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/mac8.c#L477-L499)

## Overview
Truncates a MAC-8 (EUI-64) address to its manufacturer identifier by preserving only the first 3 bytes and zeroing the remaining 5 bytes.

## Definition

```c
Datum
macaddr8_trunc(PG_FUNCTION_ARGS)
```
## Detailed Description
This function implements a truncation operation for PostgreSQL's macaddr8 data type (8-byte MAC addresses / EUI-64 identifiers). It creates a new macaddr8 structure that contains only the manufacturer portion (Organizationally Unique Identifier or OUI) of the MAC address by copying the first 3 bytes (a, b, c) from the input address and setting the remaining 5 bytes (d, e, f, g, h) to zero.

This operation allows for comparing MAC addresses based solely on their manufacturer identifier, which is useful for grouping devices by vendor or identifying the organization that assigned the MAC address range. The first 24 bits (3 bytes) of a MAC address are allocated by the IEEE Registration Authority to identify the manufacturer.

## Parameters / Member Variables
- Input: macaddr8 pointer obtained via  - the MAC address to truncate
- Returns:  - PostgreSQL function return type wrapping the resulting truncated macaddr8

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_MACADDR8_P (macro for extracting macaddr8 argument)
  - [palloc0](../p/palloc0.md) (PostgreSQL memory allocation function)
  - PG_RETURN_MACADDR8_P (macro for returning macaddr8 result)
  - macaddr8 (data structure type)
- Called from (representative examples):
  - SQL functions for manufacturer-based MAC address comparisons
  - Network analysis queries grouping devices by vendor

## Notes and Other Information
- Specifically designed for comparing macaddr8 manufacturers as noted in the source code comment
- The truncation preserves the 24-bit OUI (Organizationally Unique Identifier) portion
- Uses palloc0 to ensure the result structure is zero-initialized, though it explicitly sets the trailing bytes to 0
- In EUI-64 format, the manufacturer portion is still the first 3 bytes, same as traditional MAC addresses
- Useful for network administration and device inventory management based on manufacturer
- The resulting address can be used for manufacturer-based filtering and grouping operations