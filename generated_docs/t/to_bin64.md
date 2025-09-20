# to_bin64

## Location
[src/backend/utils/adt/varlena.c:4963-4974](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varlena.c#L4963-L4974)

## Overview
Converts a 64-bit integer value to its binary (base-2) string representation.

## Definition
```c
Datum to_bin64(PG_FUNCTION_ARGS)
```

## Detailed Description
The `to_bin64` function is a PostgreSQL SQL-callable function that takes a 64-bit signed integer and converts it to a text string containing the binary representation of that number. The function treats the input as an unsigned 64-bit value during conversion, ensuring consistent binary output regardless of the sign of the input.

Like its 32-bit counterpart `to_bin32`, this function serves as a wrapper around the internal `convert_to_base` utility function, specifically configured for base-2 conversion. It handles the full range of 64-bit integer values, making it suitable for converting large integers that exceed the 32-bit range.

## Parameters / Member Variables
- Expects one argument: a 64-bit integer value accessed via `PG_GETARG_INT64(0)`

## Dependencies
- Functions called/Symbols referenced:
  - [convert_to_base](../c/convert_to_base.md) (internal utility function for base conversion)
  - `PG_RETURN_TEXT_P` (PostgreSQL macro for returning text values)
  - `PG_GETARG_INT64` (PostgreSQL macro for extracting int64 arguments)

- Called from (representative examples):
  - No direct references found (likely called via SQL function dispatch mechanism)

## Notes and Other Information
- Located in `src/backend/utils/adt/varlena.c:4963-4974`
- Handles the full 64-bit integer range, producing up to 64 binary digits in the output
- The function explicitly casts the input to `uint64`, ensuring consistent binary representation for both positive and negative inputs
- Part of a family of base conversion functions including `to_bin32`, `to_oct32`, `to_oct64`, and `to_hex32`
- The actual conversion logic is handled by the shared `convert_to_base` static function
- Returns a PostgreSQL `text` type containing the binary string representation