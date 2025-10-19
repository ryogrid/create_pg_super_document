# macaddr8_not

## Location
[src/backend/utils/adt/mac8.c:415-433](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/mac8.c#L415-L433)

## Overview
Performs bitwise NOT operation on a MAC-8 (EUI-64) address, inverting all bits in each byte of the 8-byte address.

## Definition

```c
Datum
macaddr8_not(PG_FUNCTION_ARGS)
```
## Detailed Description
This function implements the bitwise NOT arithmetic operation for PostgreSQL's macaddr8 data type (8-byte MAC addresses / EUI-64 identifiers). It creates a new macaddr8 structure where each byte is the bitwise complement of the corresponding byte in the input address. The operation inverts every bit in the address, effectively flipping all 0s to 1s and all 1s to 0s across all 8 bytes of the MAC address.

The function allocates memory for a new macaddr8 result and performs the NOT operation on each of the 8 bytes (a through h) individually using the C bitwise NOT operator (~).

## Parameters / Member Variables
- Input: macaddr8 pointer obtained via  - the MAC address to invert
- Returns:  - PostgreSQL function return type wrapping the resulting inverted macaddr8

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_MACADDR8_P (macro for extracting macaddr8 argument)
  - [palloc0](../p/palloc0.md) (PostgreSQL memory allocation function)
  - PG_RETURN_MACADDR8_P (macro for returning macaddr8 result)
  - [macaddr8](macaddr8.md) (data structure type)
- Called from (representative examples):
  - SQL operators and functions that require bitwise NOT on MAC addresses

## Notes and Other Information
- Part of PostgreSQL's arithmetic functions for MAC-8 addresses alongside macaddr8_and and macaddr8_or
- Uses palloc0 to ensure the result structure is zero-initialized before setting values
- Each byte of the result (a, b, c, d, e, f, g, h) corresponds to one byte of the 8-byte EUI-64/MAC-8 address
- This is typically exposed to SQL users through bitwise operators for macaddr8 type

## Simplified Source

```c
macaddr8* macaddr8_not(macaddr8 *addr) {
    // Create new MAC address with all bits inverted
    macaddr8 *result = allocate_macaddr8();

    // Perform bitwise NOT on each byte of the 8-byte MAC address
    result->a = ~addr->a;
    result->b = ~addr->b;
    result->c = ~addr->c;
    result->d = ~addr->d;
    result->e = ~addr->e;
    result->f = ~addr->f;
    result->g = ~addr->g;
    result->h = ~addr->h;

    return result;
}
```