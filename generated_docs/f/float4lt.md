# float4lt

## Location
src/backend/utils/adt/float.c: 837 - 845

## Overview
PostgreSQL SQL-callable function that tests whether one single-precision floating-point number is less than another, handling NaN values according to IEEE floating-point standards.

## Definition


## Detailed Description
The  function is a PostgreSQL built-in function that implements the less-than comparison operator () for single-precision floating-point numbers (float4 type). It extracts two float4 arguments from the function call context and delegates the actual comparison logic to the  inline helper function.

The function properly handles IEEE 754 floating-point special cases, particularly NaN (Not-a-Number) values. According to IEEE standards, any comparison involving NaN returns false, except for not-equal comparisons. However, PostgreSQL's implementation treats NaN as greater than any regular number for ordering purposes.

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
-  returns true only if the first argument is not NaN AND either the second argument is NaN or the first is numerically less than the second
- This implements PostgreSQL's NaN handling policy where NaN is considered greater than all regular numbers
- Part of PostgreSQL's arithmetic data type operators system
- Used internally by the SQL parser and executor when processing float4 less-than operations