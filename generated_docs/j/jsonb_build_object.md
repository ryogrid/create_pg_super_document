# jsonb_build_object

## Location
src/backend/utils/adt/jsonb.c: 1177 - 1196

## Overview
A PostgreSQL SQL function that constructs a JSONB object from a variadic list of alternating key-value pairs.

## Definition


## Detailed Description
This function serves as the entry point for the SQL function . It accepts a variable number of arguments that must be provided in alternating key-value pairs and constructs a JSONB object from them. The function extracts the variadic arguments and delegates the actual object construction to . If an odd number of arguments is provided, an error will be raised by the worker function since keys must be paired with values.

## Parameters / Member Variables
- : Function call information structure containing the variadic arguments passed to the SQL function

## Dependencies
- Functions called/Symbols referenced:
  - [extract_variadic_args](../e/extract_variadic_args.md)
  - [jsonb_build_object_worker](jsonb_build_object_worker.md)
  - PG_RETURN_DATUM
  - PG_RETURN_NULL
- Called from (representative examples):
  - No direct callers found (SQL function entry point)

## Notes and Other Information
- This is a PostgreSQL built-in SQL function accessible via 
- The function requires an even number of arguments (alternating keys and values)
- NULL keys are not allowed and will cause an error
- NULL values are permitted and will be included in the resulting JSONB object
- The function returns NULL if no arguments are provided
- This function does not perform key uniqueness checking or skip NULL values by default