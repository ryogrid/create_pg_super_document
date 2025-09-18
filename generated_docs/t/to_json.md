# to_json

## Location
src/backend/utils/adt/json.c: 730 - 753

## Overview
The to_json function is a PostgreSQL SQL function that converts any PostgreSQL value to its JSON text representation.

## Definition


## Detailed Description
This function serves as the main entry point for converting arbitrary PostgreSQL data types to JSON format. It acts as a wrapper that determines the input data type, categorizes it appropriately for JSON conversion, and delegates the actual conversion work to datum_to_json(). The function is designed to handle any PostgreSQL data type by first analyzing the type and then applying the appropriate JSON conversion logic.

## Parameters / Member Variables
- No explicit parameters (uses PG_FUNCTION_ARGS macro)
- Accesses argument 0 as the value to convert
- Uses function call info to determine the argument type

## Dependencies
- Functions called/Symbols referenced:
  - [get_fn_expr_argtype](../g/get_fn_expr_argtype.md) (to determine input data type)
  - [json_categorize_type](../j/json_categorize_type.md) (to categorize the type for JSON conversion)
  - [datum_to_json](../d/datum_to_json.md) (to perform the actual conversion)
  - PG_RETURN_DATUM (to return the result)
- Called from:
  - Direct SQL function calls (no internal callers found)

## Notes and Other Information
- Throws an error if the input data type cannot be determined
- Located in src/backend/utils/adt/json.c:730-753
- Part of PostgreSQL's JSON functionality introduced to provide SQL-level access to JSON conversion
- The function validates that a valid data type is provided before proceeding with conversion