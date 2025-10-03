# DatumGetMacaddr8P

## Location
[src/include/utils/inet.h:163-168](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/inet.h#L163-L168)

## Overview
A static inline function that converts a PostgreSQL Datum value to a macaddr8 pointer, used for accessing MAC address data stored in PostgreSQL's internal format.

## Definition

```c
static inline macaddr8 *
DatumGetMacaddr8P(Datum X)
```
## Detailed Description
DatumGetMacaddr8P is a type conversion utility function that extracts a macaddr8 pointer from a PostgreSQL Datum. This function is part of PostgreSQL's type system infrastructure for handling MAC8 addresses (8-byte MAC addresses). It performs a simple pointer cast operation, converting the generic Datum type to a specific macaddr8 pointer type. Since macaddr8 is a fixed-length pass-by-reference datatype in PostgreSQL, this function essentially unwraps the pointer stored in the Datum.

## Parameters / Member Variables
- `X`: A PostgreSQL Datum value containing a pointer to a macaddr8 structure

## Dependencies
- Functions called/Symbols referenced:
  - [DatumGetPointer](DatumGetPointer.md) (indirectly through type cast)
  - [macaddr8](../m/macaddr8.md) (struct type)
- Called from (representative examples):
  - [convert_network_to_scalar](../c/convert_network_to_scalar.md)
  - PG_GETARG_MACADDR8_P

## Notes and Other Information
- This is a static inline function defined in src/include/utils/inet.h:163-168
- Part of PostgreSQL's type conversion infrastructure for MAC8 addresses
- The macaddr8 type represents 8-byte MAC addresses with structure members a through h
- Used in conjunction with PostgreSQL's function call interface macros
- Returns a pointer to the macaddr8 structure, not the structure itself

## Simplified Source

```c
static inline macaddr8 * DatumGetMacaddr8P(Datum X) {
    // Extract macaddr8 pointer from Datum (fixed-length type)
    return (macaddr8 *) DatumGetPointer(X);
}
```