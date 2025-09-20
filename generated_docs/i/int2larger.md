# int2larger

## Location
[src/backend/utils/adt/int.c:1346-1354](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/int.c#L1346-L1354)

## Overview
Returns the larger of two 16-bit signed integers (int16).

## Definition

```c
Datum
int2larger(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is a PostgreSQL built-in function that compares two 16-bit signed integers and returns the larger value. It implements the MAX operation for the  (smallint) data type. The function uses PostgreSQL's function calling convention with  and returns a  value containing the result.

The implementation is straightforward: it extracts two int16 arguments from the function call arguments, compares them using a simple conditional expression, and returns the larger value wrapped in a PostgreSQL Datum.

## Parameters / Member Variables
- Function uses  convention:
  - : First 16-bit signed integer (extracted from argument 0)
  - : Second 16-bit signed integer (extracted from argument 1)

## Dependencies
- Functions called/Symbols referenced:
  -  - Macro to extract int16 arguments from function call
  -  - Macro to return int16 value as Datum
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- Located in 
- This function corresponds to the SQL  function when used with two smallint values
- Part of PostgreSQL's arithmetic and comparison operators for the int2/smallint data type
- Uses standard PostgreSQL V1 function calling convention
- The comparison is performed using simple C conditional operator for efficiency