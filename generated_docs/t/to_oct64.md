# to_oct64

## Location
src/backend/utils/adt/varlena.c: 4982 - 4993

## Overview
Converts a 64-bit integer value to its octal (base-8) string representation.

## Definition
```c
Datum to_oct64(PG_FUNCTION_ARGS)
```

## Detailed Description
The `to_oct64` function is a PostgreSQL SQL-callable function that takes a 64-bit signed integer and converts it to a text string containing the octal representation of that number. The function treats the input as an unsigned 64-bit value during conversion, ensuring consistent octal output regardless of the sign of the input.

Like its 32-bit counterpart `to_oct32`, this function serves as a wrapper around the internal `convert_to_base` utility function, specifically configured for base-8 conversion. It handles the full range of 64-bit integer values, making it suitable for converting large integers that exceed the 32-bit range while maintaining the compact octal representation.

## Parameters / Member Variables
- Function follows PostgreSQL's standard function calling convention using `PG_FUNCTION_ARGS`
- Expects one argument: a 64-bit integer value accessed via `PG_GETARG_INT64(0)`
- The input is explicitly cast to `uint64` to ensure unsigned interpretation during conversion

## Dependencies
- Functions called/Symbols referenced:
  - [convert_to_base](../c/convert_to_base.md) (internal utility function for base conversion)
  - `PG_RETURN_TEXT_P` (PostgreSQL macro for returning text values)
  - `PG_GETARG_INT64` (PostgreSQL macro for extracting int64 arguments)

- Called from (representative examples):
  - No direct references found (likely called via SQL function dispatch mechanism)

## Notes and Other Information
- Located in `src/backend/utils/adt/varlena.c:4982-4993`
- Handles the full 64-bit integer range, producing up to 22 octal digits in the output (compared to 11 for 32-bit values)
- The function explicitly casts the input to `uint64`, ensuring consistent octal representation for both positive and negative inputs
- Octal representation is more compact than binary but less compact than hexadecimal, providing a good balance for readability in certain contexts
- Part of a family of base conversion functions including `to_bin32`, `to_bin64`, `to_oct32`, and `to_hex32`
- The actual conversion logic is handled by the shared `convert_to_base` static function
- Returns a PostgreSQL `text` type containing the octal string representation
- Particularly useful for large file permissions, system identifiers, and other contexts where octal notation is preferred over decimal