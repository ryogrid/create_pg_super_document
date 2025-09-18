# float48le

## Location
[src/backend/utils/adt/float.c:3891-3899](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/float.c#L3891-L3899)

## Overview
PostgreSQL function that performs less-than-or-equal comparison between a float4 (single precision) and a float8 (double precision) value.

## Definition


## Detailed Description
The  function implements the less-than-or-equal comparison operator for mixed-precision floating point types in PostgreSQL. It takes a float4 (4-byte single precision float) as the first argument and a float8 (8-byte double precision float) as the second argument, then determines if the first value is less than or equal to the second value.

The function works by:
1. Extracting the float4 value from the first function argument
2. Extracting the float8 value from the second function argument  
3. Converting the float4 to float8 precision via casting
4. Delegating the actual comparison to the  function
5. Returning the boolean result

This function is part of PostgreSQL's type system that allows seamless comparison operations between different numeric precision types.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  - : float4 value (single precision floating point number)
  - : float8 value (double precision floating point number)

## Dependencies
- Functions called/Symbols referenced:
  - : Macro to extract float4 from function arguments
  - : Macro to extract float8 from function arguments
  - : Core function that performs less-than-or-equal comparison on two float8 values
  - : Macro to return boolean result
- Called from (representative examples):
  - No direct references found in the analyzed codebase

## Notes and Other Information
- Located in 
- Part of PostgreSQL's arithmetic data type (ADT) system
- Handles mixed-precision comparisons by promoting the lower precision operand
- The actual comparison logic is delegated to  after type promotion
- Returns a Datum-wrapped boolean value as per PostgreSQL's function call convention