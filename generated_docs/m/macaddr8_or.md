# macaddr8_or

## Location
[src/backend/utils/adt/mac8.c:454-476](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/mac8.c#L454-L476)

## Overview
Performs bitwise OR operation between two MAC-8 (EUI-64) addresses, combining corresponding bits using logical OR across all 8 bytes.

## Definition

```c
Datum
macaddr8_or(PG_FUNCTION_ARGS)
```
## Detailed Description
This function implements the bitwise OR arithmetic operation for PostgreSQL's macaddr8 data type (8-byte MAC addresses / EUI-64 identifiers). It takes two macaddr8 input addresses and creates a new macaddr8 structure where each byte is the result of performing bitwise OR between the corresponding bytes of the input addresses. The operation combines bits such that the result bit is 1 when either input bit (or both) is 1, and 0 only when both input bits are 0.

The function allocates memory for a new macaddr8 result and performs the OR operation on each of the 8 bytes (a through h) individually using the C bitwise OR operator (|).

## Parameters / Member Variables
- Input 1: macaddr8 pointer obtained via  - first MAC address operand
- Input 2: macaddr8 pointer obtained via  - second MAC address operand
- Returns:  - PostgreSQL function return type wrapping the resulting macaddr8 from OR operation

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_MACADDR8_P (macro for extracting macaddr8 arguments)
  - [palloc0](../p/palloc0.md) (PostgreSQL memory allocation function)
  - PG_RETURN_MACADDR8_P (macro for returning macaddr8 result)
  - [macaddr8](macaddr8.md) (data structure type)
- Called from (representative examples):
  - SQL operators and functions that require bitwise OR on MAC addresses

## Notes and Other Information
- Part of PostgreSQL's arithmetic functions for MAC-8 addresses alongside macaddr8_not and macaddr8_and
- Uses palloc0 to ensure the result structure is zero-initialized before setting values
- Each byte of the result (a, b, c, d, e, f, g, h) corresponds to one byte of the 8-byte EUI-64/MAC-8 address
- This is typically exposed to SQL users through bitwise operators for macaddr8 type
- Useful for combining MAC addresses or setting specific bits across multiple addresses
- Complements macaddr8_and for complete bitwise manipulation capabilities

## Simplified Source

```c
macaddr8* macaddr8_or(macaddr8 *addr1, macaddr8 *addr2) {
    // Create new MAC address with bitwise OR of two input addresses
    macaddr8 *result = allocate_macaddr8();

    // Perform bitwise OR on each byte of the 8-byte MAC addresses
    result->a = addr1->a | addr2->a;
    result->b = addr1->b | addr2->b;
    result->c = addr1->c | addr2->c;
    result->d = addr1->d | addr2->d;
    result->e = addr1->e | addr2->e;
    result->f = addr1->f | addr2->f;
    result->g = addr1->g | addr2->g;
    result->h = addr1->h | addr2->h;

    return result;
}
```