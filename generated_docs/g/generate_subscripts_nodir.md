# generate_subscripts_nodir

## Location
[src/backend/utils/adt/arrayfuncs.c:5969-5979](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/arrayfuncs.c#L5969-L5979)

## Overview
A wrapper function that implements the 2-argument version of the generate_subscripts PostgreSQL function, which returns all subscripts of an array for a specified dimension.

## Definition

```c
Datum
generate_subscripts_nodir(PG_FUNCTION_ARGS)
```
## Detailed Description
This function serves as a simple wrapper around the main `generate_subscripts` function. It exists to provide compatibility for the 2-argument variant of `generate_subscripts(array, dim)` where the reverse parameter is not specified. The function directly delegates to `generate_subscripts`, which can handle both the 2-argument and 3-argument versions of the call.

The function is part of PostgreSQL's array manipulation functionality and is used internally to support the SQL function `generate_subscripts(anyarray, int)` which generates a series of valid subscripts for a given array dimension.

## Parameters / Member Variables
The function uses the standard PostgreSQL function call interface:
- Uses `PG_FUNCTION_ARGS` macro to access function arguments
- Arguments are passed through to the main `generate_subscripts` function:
  - Argument 0: Array (anyarray type)  
  - Argument 1: Dimension number (integer)
  - No reverse parameter (defaults to false in main function)

## Dependencies
- Functions called/Symbols referenced:
  - [generate_subscripts](generate_subscripts.md)
- Called from (representative examples):
  - Used as PostgreSQL function implementation (no direct code references found)

## Notes and Other Information
- This is a thin wrapper function with minimal overhead
- The actual functionality is implemented in the main `generate_subscripts` function
- Part of PostgreSQL's Set Returning Function (SRF) infrastructure
- Located in src/backend/utils/adt/arrayfuncs.c at lines 5969-5979
- Provides backward compatibility for the simpler 2-argument interface