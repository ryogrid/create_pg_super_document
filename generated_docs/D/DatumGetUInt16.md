# DatumGetUInt16

## Location
[src/include/postgres.h:182-191](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/postgres.h#L182-L191)

## Overview
DatumGetUInt16 is an inline function that extracts a 16-bit unsigned integer value from a PostgreSQL Datum type.

## Definition
```c
static inline uint16
DatumGetUInt16(Datum X)
```

## Detailed Description
DatumGetUInt16 is a type conversion function that casts a Datum value to a 16-bit unsigned integer (uint16). It performs a direct cast operation without any validation or range checking. This function is part of PostgreSQL's datum conversion utility functions that facilitate type conversions between the generic Datum type and specific C data types.

The function is declared as static inline, meaning it will be inlined by the compiler for performance optimization, avoiding function call overhead for this simple operation. This function has more limited usage compared to its signed counterpart DatumGetInt16, being primarily used in specialized contexts.

## Parameters / Member Variables
- `X`: The input Datum value to be converted to uint16

## Dependencies
- Functions called/Symbols referenced: None (simple cast operation)
- Called from (representative examples):
  - PG_GETARG_UINT16 (function argument extraction macro)
  - [gintuple_get_attrnum](../g/gintuple_get_attrnum.md) (GIN index utility function)

## Notes and Other Information
- This is a low-level utility function for type conversion
- No bounds checking is performed - the caller is responsible for ensuring the Datum contains a valid uint16 value
- The function is defined in src/include/postgres.h:182-191
- Part of the family of DatumGet* conversion functions in PostgreSQL
- Less commonly used than DatumGetInt16, primarily for specialized unsigned integer operations
- Used in GIN index operations and function argument processing where unsigned 16-bit values are needed