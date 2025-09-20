# float4gt

## Location
[src/backend/utils/adt/float.c:855-863](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/float.c#L855-L863)

## Overview
PostgreSQL SQL-callable function that tests whether one single-precision floating-point number is greater than another, handling NaN values according to PostgreSQL's floating-point ordering conventions.

## Definition

```c
Datum
float4gt(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is a PostgreSQL built-in function that implements the greater-than comparison operator () for single-precision floating-point numbers (float4 type). It extracts two float4 arguments from the function call context and delegates the actual comparison logic to the  inline helper function.

The function handles IEEE 754 floating-point special cases, particularly NaN (Not-a-Number) values. PostgreSQL's implementation treats NaN as greater than any regular number for ordering purposes, meaning NaN > regular_number returns true, but regular_number > NaN returns false.

## Parameters / Member Variables
- Function follows PostgreSQL's  calling convention
- : First float4 operand extracted via 
- : Second float4 operand extracted via 

## Dependencies
- Functions called/Symbols referenced:
  -  (macro for extracting float4 arguments)
  -  (inline helper function that performs the actual comparison)
  -  (macro for returning boolean result)
- Called from:
  - SQL queries using the  operator with float4 operands
  - Internal PostgreSQL expression evaluation system

## Notes and Other Information
- Located in 
- The actual comparison logic is implemented in  helper function in 
-  returns true only if the second argument is not NaN AND either the first argument is NaN or the first is numerically greater than the second
- This implements PostgreSQL's NaN handling policy where NaN is considered greater than all regular numbers
- Part of PostgreSQL's arithmetic data type operators system
- Used internally by the SQL parser and executor when processing float4 greater-than operations