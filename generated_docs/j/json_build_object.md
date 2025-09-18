# json_build_object

## Location
src/backend/utils/adt/json.c: 1309 - 1328

## Overview
The json_build_object function is a SQL function that creates a JSON object from a variadic list of alternating key-value pairs.

## Definition
```c
Datum json_build_object(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the PostgreSQL SQL function json_build_object(variadic "any"). It takes a variable number of arguments representing alternating keys and values, and constructs a JSON object from them. The function extracts the variadic arguments and delegates the actual JSON object construction to the json_build_object_worker function with default parameters (no absent_on_null handling and no unique key checking).

## Parameters / Member Variables
- PG_FUNCTION_ARGS: Standard PostgreSQL function calling convention that provides access to the function call information including arguments, their types, and null flags

## Dependencies
- Functions called/Symbols referenced:
  - extract_variadic_args: Extracts arguments from the variadic function call
  - json_build_object_worker: Performs the actual JSON object construction
  - PG_RETURN_DATUM: PostgreSQL macro for returning a Datum value
  - PG_RETURN_NULL: PostgreSQL macro for returning NULL

- Called from (representative examples):
  - No direct references found in the codebase (called via SQL interface)

## Notes and Other Information
- The function serves as a wrapper that extracts variadic arguments and passes them to the worker function
- If argument extraction fails (nargs < 0), the function returns NULL
- The actual JSON object construction logic is handled by json_build_object_worker with parameters set to disable absent_on_null behavior and unique key checking
- This is part of PostgreSQL JSON data type support system located in src/backend/utils/adt/json.c:1309-1328