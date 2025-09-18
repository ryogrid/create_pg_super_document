# anyarray_out

## Location
src/backend/utils/adt/pseudotypes.c: 158 - 163

## Overview
The `anyarray_out` function is an output conversion function for the `anyarray` pseudo-type in PostgreSQL, serving as a wrapper that delegates to the standard array output function.

## Definition
```c
Datum anyarray_out(PG_FUNCTION_ARGS)
```

## Detailed Description
The `anyarray_out` function serves as the output conversion function for PostgreSQL's `anyarray` pseudo-type. It is implemented as a simple wrapper function that directly delegates all processing to the `array_out` function by passing through the function call information (`fcinfo`). The `anyarray` pseudo-type is used in polymorphic functions to represent array types of any element type, and this function ensures that when an `anyarray` value needs to be output as text, it uses the same logic as regular array output formatting. This design maintains consistency in array representation regardless of whether the array is accessed through the generic `anyarray` interface or a specific array type.

## Parameters / Member Variables
- The function follows PostgreSQL's standard function calling convention using `PG_FUNCTION_ARGS`, which provides access to:
  - Input parameter: An anyarray value that represents an array of any element type
  - The function passes the entire `fcinfo` (function call information) to `array_out`

## Dependencies
- Functions called/Symbols referenced:
  - [array_out](array_out.md) (located in `src/backend/utils/adt/arrayfuncs.c:1016-1200`) - the main array output function that handles array-to-text conversion
- Called from (representative examples):
  - Type system operations when outputting anyarray pseudo-type values
  - Polymorphic function return value formatting

## Notes and Other Information
- This function is a simple delegation wrapper - all actual array formatting logic is handled by `array_out`
- The `anyarray` pseudo-type is used in polymorphic functions to accept arrays of any element type
- By delegating to `array_out`, it ensures consistent array text representation across all array types
- Located in `src/backend/utils/adt/pseudotypes.c:158-163`
- Part of PostgreSQL's polymorphic type system that allows functions to work with multiple data types