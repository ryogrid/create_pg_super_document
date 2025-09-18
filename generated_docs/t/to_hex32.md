# to_hex32

## Location
[src/backend/utils/adt/varlena.c:4994-5000](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varlena.c#L4994-L5000)

## Overview
Converts a 32-bit integer value to its hexadecimal (base-16) string representation.

## Definition
```c
Datum to_hex32(PG_FUNCTION_ARGS)
```

## Detailed Description
The `to_hex32` function is a PostgreSQL SQL-callable function that takes a 32-bit signed integer and converts it to a text string containing the hexadecimal representation of that number. The function treats the input as an unsigned 32-bit value during conversion, ensuring consistent hexadecimal output regardless of the sign of the input.

This function serves as a wrapper around the internal `convert_to_base` utility function, specifically configured for base-16 conversion. Hexadecimal representation uses digits 0-9 and letters a-f (lowercase), providing the most compact readable representation of binary data. This makes it particularly useful for displaying memory addresses, bit patterns, and other low-level data representations.

## Parameters / Member Variables
- Function follows PostgreSQL's standard function calling convention using `PG_FUNCTION_ARGS`
- Expects one argument: a 32-bit integer value accessed via `PG_GETARG_INT32(0)`
- The input is cast to `uint32` to ensure unsigned interpretation before conversion to `uint64`

## Dependencies
- Functions called/Symbols referenced:
  - [convert_to_base](../c/convert_to_base.md) (internal utility function for base conversion)
  - `PG_RETURN_TEXT_P` (PostgreSQL macro for returning text values)
  - `PG_GETARG_INT32` (PostgreSQL macro for extracting int32 arguments)

- Called from (representative examples):
  - No direct references found (likely called via SQL function dispatch mechanism)

## Notes and Other Information
- Located in `src/backend/utils/adt/varlena.c:4994-5000`
- The function explicitly casts the input to `uint32` before widening to `uint64`, ensuring consistent hexadecimal representation
- Hexadecimal representation is the most compact readable format, requiring at most 8 digits for a 32-bit value
- Uses lowercase letters (a-f) for hex digits 10-15, as implemented by the `convert_to_base` function
- Part of a family of base conversion functions including `to_bin32`, `to_bin64`, `to_oct32`, and `to_oct64`
- The actual conversion logic is handled by the shared `convert_to_base` static function
- Returns a PostgreSQL `text` type containing the hexadecimal string representation
- Particularly useful for debugging, displaying memory addresses, color codes, and other contexts where hexadecimal notation is the standard representation