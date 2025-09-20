# float4abs

## Location
[src/backend/utils/adt/float.c:584-594](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/float.c#L584-L594)

## Overview
Computes the absolute value of a single-precision floating-point number (float4) in PostgreSQL.

## Definition

```c
Datum
float4abs(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is a PostgreSQL built-in function that returns the absolute value of a float4 (single-precision floating-point) argument. It uses the standard C library function  to compute the absolute value and returns the result as a PostgreSQL Datum. This function is part of the float4 base operations in PostgreSQL's arithmetic system.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  -  - macro to extract float4 argument from function call
  -  - macro to return float4 result as Datum  
  -  - standard C library function for single-precision absolute value
  -  - PostgreSQL type for single-precision floating-point numbers
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- Located in src/backend/utils/adt/float.c:584-594
- Part of PostgreSQL's float4 base operations
- Uses standard C library math functions for the actual computation
- Follows PostgreSQL's function calling convention using PG_FUNCTION_ARGS macro
- Returns result using PostgreSQL's Datum system for type-safe value passing