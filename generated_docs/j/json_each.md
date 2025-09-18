# json_each

## Location
[src/backend/utils/adt/jsonfuncs.c:1948-1953](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonfuncs.c#L1948-L1953)

## Overview
A SQL function that decomposes a JSON object into key-value pairs as a set-returning function.

## Definition
```c
Datum json_each(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the PostgreSQL SQL function `json_each` which takes a JSON object as input and returns a set of records containing key-value pairs. Unlike `json_object_keys()` which only returns keys, this function returns both keys and their corresponding values from the JSON object.

The function operates in materialize mode, meaning it stashes all results into a Tuplestore object as it processes the JSON object. It uses a temporary memory context that is cleared after each tuple is built to manage memory efficiently during the decomposition process.

This is a set-returning function (SRF) that can be used in SQL queries to extract and work with individual key-value pairs from JSON objects.

## Parameters / Member Variables
- Uses the standard PostgreSQL function argument structure `PG_FUNCTION_ARGS` which contains:
  - The JSON object to decompose
  - Function call context information

## Dependencies
- Functions called/Symbols referenced:
  - [each_worker](../e/each_worker.md) (the core implementation function, called with false parameter)
  - PG_FUNCTION_ARGS (PostgreSQL function argument macro)
  - Datum (PostgreSQL return type)
- Called from (representative examples):
  - SQL queries using the json_each() function
  - No direct C code references found

## Notes and Other Information
- This is a user-facing SQL function exposed in PostgreSQL's JSON functionality
- Returns Datum type following PostgreSQL's function calling conventions
- Works specifically with JSON objects (not arrays or scalars)
- The `false` parameter passed to each_worker distinguishes it from json_each_text
- Part of PostgreSQL's comprehensive JSON processing capabilities
- Materialize mode ensures all results are available before returning to the caller
- Uses temporary memory contexts for efficient memory management during tuple construction