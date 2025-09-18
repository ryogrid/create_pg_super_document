# int2and

## Location
[src/backend/utils/adt/int.c:1446-1454](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/int.c#L1446-L1454)

## Overview
Performs bitwise AND operation between two 16-bit signed integers (smallint type in PostgreSQL).

## Definition


## Detailed Description
The  function implements the bitwise AND operation for PostgreSQL's  data type (16-bit signed integers). It takes two  arguments from the function call context, performs a bitwise AND operation using the C  operator, and returns the result as a  value. This function is typically invoked through PostgreSQL's SQL operator  when used with  operands.

## Parameters / Member Variables
- Uses  macro to access function arguments from PostgreSQL's function call context
- : First 16-bit signed integer operand retrieved via 
- : Second 16-bit signed integer operand retrieved via 

## Dependencies
- Functions called/Symbols referenced:
  - : Macro to extract int16 arguments from function call context
  - : Macro to return int16 result from PostgreSQL function
- Called from (representative examples):
  - No direct references found (likely called through PostgreSQL's operator dispatch system)

## Notes and Other Information
- Located in 
- Part of PostgreSQL's arithmetic and bitwise operations for integer types
- The function follows PostgreSQL's standard function interface using  and return macros
- Typically accessed through the SQL bitwise AND operator  rather than direct function calls