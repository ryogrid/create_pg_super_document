# jsonb_each

## Location
[src/backend/utils/adt/jsonfuncs.c:1954-1959](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonfuncs.c#L1954-L1959)

## Overview
A SQL function that decomposes a JSONB object into key-value pairs as a set-returning function.

## Definition
```c
Datum jsonb_each(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the PostgreSQL SQL function `jsonb_each` which takes a JSONB (binary JSON) object as input and returns a set of records containing key-value pairs. It is the JSONB equivalent of the `json_each` function, working with PostgreSQL's binary JSON format rather than text-based JSON.

The function operates similarly to `json_each` but works with the more efficient binary JSONB format, which allows for faster processing and better indexing capabilities. Like its JSON counterpart, it operates in materialize mode and uses a Tuplestore to manage the result set.

This is a set-returning function (SRF) that can be used in SQL queries to extract and work with individual key-value pairs from JSONB objects.

## Parameters / Member Variables
- Uses the standard PostgreSQL function argument structure `PG_FUNCTION_ARGS` which contains:

## Dependencies
- Functions called/Symbols referenced:
  - [each_worker_jsonb](../e/each_worker_jsonb.md) (the core implementation function for JSONB, called with "jsonb_each" name and false parameter)
  - PG_FUNCTION_ARGS (PostgreSQL function argument macro)
  - Datum (PostgreSQL return type)
- Called from (representative examples):
  - SQL queries using the jsonb_each() function
  - No direct C code references found

## Notes and Other Information
- This is a user-facing SQL function exposed in PostgreSQL's JSONB functionality
- Returns Datum type following PostgreSQL's function calling conventions
- Works specifically with JSONB objects (binary JSON format)
- The `false` parameter passed to each_worker_jsonb distinguishes it from jsonb_each_text
- The function name "jsonb_each" is passed as a string parameter for error reporting and identification
- More efficient than json_each when working with JSONB data due to the binary format
- Part of PostgreSQL's comprehensive JSONB processing capabilities
- JSONB format allows for better indexing and faster operations compared to text-based JSON