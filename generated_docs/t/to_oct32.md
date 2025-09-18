# to_oct32

## Location
[src/backend/utils/adt/varlena.c:4975-4981](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varlena.c#L4975-L4981)

## Overview
Converts a 32-bit integer value to its octal (base-8) string representation.

## Definition
```c
Datum to_oct32(PG_FUNCTION_ARGS)
```

## Detailed Description
The `to_oct32` function is a PostgreSQL SQL-callable function that takes a 32-bit signed integer and converts it to a text string containing the octal representation of that number. The function treats the input as an unsigned 32-bit value during conversion, ensuring consistent octal output regardless of the sign of the input.

This function serves as a wrapper around the internal `convert_to_base` utility function, specifically configured for base-8 conversion. Octal representation uses digits 0-7 and is commonly used in system programming and Unix file permissions, making this function useful for applications that need to display or process octal values.

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
- Located in `src/backend/utils/adt/varlena.c:4975-4981`
- The function explicitly casts the input to `uint32` before widening to `uint64`, ensuring consistent octal representation
- Octal representation is more compact than binary, requiring at most 11 digits for a 32-bit value (compared to 32 for binary)
- Part of a family of base conversion functions including `to_bin32`, `to_bin64`, `to_oct64`, and `to_hex32`
- The actual conversion logic is handled by the shared `convert_to_base` static function
- Returns a PostgreSQL `text` type containing the octal string representation
- Useful for Unix-style file permissions and other system-level programming contexts where octal notation is preferred