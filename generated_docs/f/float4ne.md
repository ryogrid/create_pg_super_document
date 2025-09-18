# float4ne

## Location
[src/backend/utils/adt/float.c:828-836](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/float.c#L828-L836)

## Overview
PostgreSQL SQL-callable function that tests whether two single-precision floating-point numbers are not equal, handling NaN values according to IEEE floating-point standards.

## Definition


## Detailed Description
The  function is a PostgreSQL built-in function that implements the not-equal comparison operator ( or ) for single-precision floating-point numbers (float4 type). It extracts two float4 arguments from the function call context and delegates the actual comparison logic to the  inline helper function.

The function properly handles IEEE 754 floating-point special cases, particularly NaN (Not-a-Number) values. According to IEEE standards, any comparison involving NaN should behave specially - two NaN values are considered not equal to each other, and NaN is not equal to any regular number.

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
  - SQL queries using the  or  operators with float4 operands
  - Internal PostgreSQL expression evaluation system

## Notes and Other Information
- Located in 
- The actual comparison logic is implemented in  helper function in 
-  handles NaN values specially: returns true if one operand is NaN and the other is not, or if both are regular numbers and not equal
- Part of PostgreSQL's arithmetic data type operators system
- Used internally by the SQL parser and executor when processing float4 not-equal operations