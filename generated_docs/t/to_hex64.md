# to_hex64

## Location
[src/backend/utils/adt/varlena.c:5001-5013](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varlena.c#L5001-L5013)

## Overview
Converts a 64-bit integer value to its hexadecimal string representation.

## Definition

```c
Datum
to_hex64(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is a PostgreSQL built-in function that takes a 64-bit integer (bigint) as input and converts it to a hexadecimal string representation. It uses the internal  utility function with base 16 to perform the conversion. The function follows PostgreSQL's standard function calling convention using the  macro.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure
  - Argument 0: A 64-bit signed integer (bigint) to be converted to hexadecimal

## Dependencies
- Functions called/Symbols referenced:
  - : Macro to extract a 64-bit integer argument
  - : Internal utility function for base conversion
  - : Macro to return a text result
- Called from (representative examples):
  - No direct references found in the codebase (likely called via SQL function interface)

## Notes and Other Information
- Located in 
- This function is typically exposed as a SQL function  for converting 64-bit integers
- The conversion treats the input as an unsigned 64-bit value for the hexadecimal representation
- Part of PostgreSQL's suite of base conversion functions for different data types

## Simplified Source

```c
Datum
to_hex64(PG_FUNCTION_ARGS)
{
    // Extract 64-bit integer as unsigned for consistent hex representation
    uint64 value = (uint64) PG_GETARG_INT64(0);

    // Convert to hexadecimal (base-16) string and return as text
    PG_RETURN_TEXT_P(convert_to_base(value, 16));
}
```