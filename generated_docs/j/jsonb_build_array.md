# jsonb_build_array

## Location
[src/backend/utils/adt/jsonb.c:1237-1257](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonb.c#L1237-L1257)

## Overview
A PostgreSQL SQL function that constructs a JSONB array from a variadic list of arguments.

## Definition

```c
Datum
jsonb_build_array(PG_FUNCTION_ARGS)
```
## Detailed Description
This function serves as the entry point for the SQL function . It accepts a variable number of arguments and constructs a JSONB array containing those values in the order they were provided. The function extracts the variadic arguments and delegates the actual array construction to . Unlike object construction, array construction accepts any number of arguments (including zero) and doesn't require pairing.

## Parameters / Member Variables
- : Function call information structure containing the variadic arguments passed to the SQL function

## Dependencies
- Functions called/Symbols referenced:
  - [extract_variadic_args](../e/extract_variadic_args.md)
  - [jsonb_build_array_worker](jsonb_build_array_worker.md)
  - PG_RETURN_DATUM
  - PG_RETURN_NULL
- Called from (representative examples):
  - No direct callers found (SQL function entry point)

## Notes and Other Information
- This is a PostgreSQL built-in SQL function accessible via 
- The function accepts any number of arguments, including zero
- NULL values are included in the resulting JSONB array as JSON null values
- The function returns NULL if argument extraction fails
- Arguments are processed in the order they are provided
- This function does not skip NULL values by default (uses )