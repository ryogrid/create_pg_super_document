# make_const

## Location
src/backend/parser/parse_node.c: 347 - 480

## Overview
A function that converts raw constant values from the parser (A_Const nodes) into typed Const nodes by determining the "natural" type for each constant when no explicit cast is provided.

## Definition


## Detailed Description
This function serves as the primary converter for literal constants during the parsing phase. When the parser encounters a constant value without an explicit type cast, make_const determines the most appropriate PostgreSQL data type and creates the corresponding Const node.

The function handles several categories of constants:

1. **NULL Values**: Always typed as UNKNOWN, allowing later type resolution to determine the appropriate type based on context.

2. **Integers**: Initially stored as int4 (INT4OID). The function attempts to fit values into int32, but can also produce int8 for larger values.

3. **Floats**: Uses sophisticated logic to handle both genuine floating-point numbers and integers that exceed int32 range. It first attempts to parse as int64, falling back to numeric type for true floating-point values or very large integers.

4. **Booleans**: Directly converted to bool type (BOOLOID).

5. **Strings**: Typed as UNKNOWN to allow subsequent type coercion based on usage context.

6. **Bit Strings**: Converted to bit type (BITOID) using the bit_in function.

The function includes comprehensive error handling using parser error position callbacks to provide accurate error location information when type conversion functions fail.

## Parameters / Member Variables
- : Parse state containing context information for error reporting and location tracking
- : The A_Const node from the parser containing the raw constant value and metadata

## Dependencies
- Functions called/Symbols referenced:
  - [A_Const](../A/A_Const.md) (input structure type)
  - [makeConst](makeConst.md)() (creates the final Const node)
  - nodeTag() (determines the type of the constant value)
  - intVal()/boolVal()/strVal() (extracts values from ValUnion)
  - [pg_strtoint64_safe](../p/pg_strtoint64_safe.md)() (safe integer parsing)
  - DirectFunctionCall3() (calls type input functions)
  - [numeric_in](../n/numeric_in.md)() (converts string to numeric type)
  - [bit_in](../b/bit_in.md)() (converts string to bit type)
  - [setup_parser_errposition_callback](../s/setup_parser_errposition_callback.md)() (error location reporting)
  - [cancel_parser_errposition_callback](../c/cancel_parser_errposition_callback.md)() (cleanup error callback)
  - Various type OID constants (INT4OID, INT8OID, NUMERICOID, etc.)
- Called from (representative examples):
  - [transformExprRecurse](../t/transformExprRecurse.md)() (during expression transformation)

## Notes and Other Information
- The function implements PostgreSQL's "natural" type inference for uncast constants
- String literals and NULL are initially typed as UNKNOWN to allow flexible type coercion later
- The function includes special logic for handling integers that might fit in different-sized integer types
- Error position callbacks ensure that type conversion errors report the correct location in the source SQL
- The function preserves the original source location from the A_Const node in the resulting Const node
- All created constants use typmod -1 and are considered uncollatable unless specifically handled
- Location: src/backend/parser/parse_node.c:347-480