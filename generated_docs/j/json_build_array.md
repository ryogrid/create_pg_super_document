# json_build_array

## Location
src/backend/utils/adt/json.c: 1365 - 1384

## Overview
The json_build_array function is a SQL function that creates a JSON array from a variadic list of arguments of any type.

## Definition
```c
Datum json_build_array(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the PostgreSQL SQL function json_build_array(variadic "any"). It accepts a variable number of arguments of any PostgreSQL data type and constructs a JSON array containing those values. The function extracts the variadic arguments and delegates the actual JSON array construction to the json_build_array_worker function with absent_on_null set to false, meaning null values will be included in the array as JSON null values.

## Parameters / Member Variables
- PG_FUNCTION_ARGS: Standard PostgreSQL function calling convention that provides access to the function call information including arguments, their types, and null flags

## Dependencies
- Functions called/Symbols referenced:
  - extract_variadic_args: Extracts arguments from the variadic function call
  - json_build_array_worker: Performs the actual JSON array construction
  - PG_RETURN_DATUM: PostgreSQL macro for returning a Datum value
  - PG_RETURN_NULL: PostgreSQL macro for returning NULL

- Called from (representative examples):
  - No direct references found in the codebase (called via SQL interface)

## Notes and Other Information
- The function serves as a wrapper that extracts variadic arguments and passes them to the worker function
- If argument extraction fails (nargs < 0), the function returns NULL
- The absent_on_null parameter is set to false, so null values are included as JSON null in the array
- Unlike json_build_object, this function does not require an even number of arguments since arrays accept any number of elements
- The actual JSON array construction logic is handled by json_build_array_worker
- This is part of PostgreSQL JSON data type support system located in src/backend/utils/adt/json.c:1365-1384