# float8out

## Location
[src/backend/utils/adt/float.c:515-529](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/float.c#L515-L529)

## Overview
PostgreSQL system function that converts a float8 (double precision) value to its string representation using the standard output format.

## Definition

```c
Datum
float8out(PG_FUNCTION_ARGS)
```
## Detailed Description
This function serves as the PostgreSQL system interface for converting float8 values to string format. It acts as a thin wrapper around , handling the PostgreSQL function call protocol by extracting the float8 argument and returning the result as a C string Datum. This function is typically registered in the system catalogs as the output function for the float8 data type.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_FLOAT8 (macro to extract float8 argument)
  - [float8out_internal](float8out_internal.md) (internal implementation for float8 to string conversion)
  - PG_RETURN_CSTRING (macro to return C string as Datum)

- Called from (representative examples):
  - System catalog functions (registered as output function for float8 type)
  - No direct references found in indexed code

## Notes and Other Information
- Part of PostgreSQL's type system infrastructure
- Registered in system catalogs as the output function for float8 data type
- Simple wrapper that delegates actual work to float8out_internal()
- Returns result compatible with PostgreSQL's Datum system
- Used automatically by PostgreSQL when converting float8 values to text in queries, COPY operations, etc.