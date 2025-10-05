# jsonb_build_object

## Location
[src/backend/utils/adt/jsonb.c:1177-1196](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonb.c#L1177-L1196)

## Overview
A PostgreSQL SQL function that constructs a JSONB object from a variadic list of alternating key-value pairs.

## Definition

```c
Datum
jsonb_build_object(PG_FUNCTION_ARGS)
```
## Detailed Description
This function serves as the entry point for the SQL function . It accepts a variable number of arguments that must be provided in alternating key-value pairs and constructs a JSONB object from them. The function extracts the variadic arguments and delegates the actual object construction to . If an odd number of arguments is provided, an error will be raised by the worker function since keys must be paired with values.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Function call information structure containing the variadic arguments passed to the SQL function
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

## Simplified Source

```c
Datum
jsonb_build_object(PG_FUNCTION_ARGS)
{
    // Extract variadic arguments as key-value pairs
    Datum *args;
    bool *nulls;
    Oid *types;

    int nargs = extract_variadic_args(fcinfo, 0, true, &args, &types, &nulls);

    // Return NULL if no arguments provided
    if (nargs < 0)
        PG_RETURN_NULL();

    // Build the JSONB object using worker function
    PG_RETURN_DATUM(jsonb_build_object_worker(nargs, args, nulls, types, false, false));
}
```