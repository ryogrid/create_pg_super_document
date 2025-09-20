# macaddr8_and

## Location
[src/backend/utils/adt/mac8.c:434-453](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/mac8.c#L434-L453)

## Overview
Performs bitwise AND operation between two MAC-8 (EUI-64) addresses, combining corresponding bits using logical AND across all 8 bytes.

## Definition

```c
Datum
macaddr8_and(PG_FUNCTION_ARGS)
```
## Detailed Description
This function implements the bitwise AND arithmetic operation for PostgreSQL's macaddr8 data type (8-byte MAC addresses / EUI-64 identifiers). It takes two macaddr8 input addresses and creates a new macaddr8 structure where each byte is the result of performing bitwise AND between the corresponding bytes of the input addresses. The operation combines bits such that the result bit is 1 only when both input bits are 1, otherwise it's 0.

The function allocates memory for a new macaddr8 result and performs the AND operation on each of the 8 bytes (a through h) individually using the C bitwise AND operator (&).

## Parameters / Member Variables
- Input 1: macaddr8 pointer obtained via  - first MAC address operand
- Input 2: macaddr8 pointer obtained via  - second MAC address operand  
- Returns:  - PostgreSQL function return type wrapping the resulting macaddr8 from AND operation

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_MACADDR8_P (macro for extracting macaddr8 arguments)
  - [palloc0](../p/palloc0.md) (PostgreSQL memory allocation function)
  - PG_RETURN_MACADDR8_P (macro for returning macaddr8 result)
  - macaddr8 (data structure type)
- Called from (representative examples):
  - SQL operators and functions that require bitwise AND on MAC addresses

## Notes and Other Information
- Part of PostgreSQL's arithmetic functions for MAC-8 addresses alongside macaddr8_not and macaddr8_or
- Uses palloc0 to ensure the result structure is zero-initialized before setting values
- Each byte of the result (a, b, c, d, e, f, g, h) corresponds to one byte of the 8-byte EUI-64/MAC-8 address
- This is typically exposed to SQL users through bitwise operators for macaddr8 type
- Useful for applying bit masks to MAC addresses or finding common bits between addresses