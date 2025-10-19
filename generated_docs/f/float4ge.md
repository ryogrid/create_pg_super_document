# float4ge

## Location
[src/backend/utils/adt/float.c:864-872](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/float.c#L864-L872)

## Overview
PostgreSQL SQL-callable function that tests whether one single-precision floating-point number is greater than or equal to another, handling NaN values according to PostgreSQL's floating-point ordering conventions.

## Definition

```c
Datum
float4ge(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is a PostgreSQL built-in function that implements the greater-than-or-equal-to comparison operator () for single-precision floating-point numbers (float4 type). It extracts two float4 arguments from the function call context and delegates the actual comparison logic to the  inline helper function.

The function handles IEEE 754 floating-point special cases, particularly NaN (Not-a-Number) values. PostgreSQL's implementation treats NaN as greater than any regular number for ordering purposes, meaning NaN >= regular_number returns true, but regular_number >= NaN returns false only when the regular number is not equal to NaN.

## Parameters / Member Variables
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
-  returns true if the first argument is NaN, or if the second argument is not NaN and the first is greater than or equal to the second
- This implements PostgreSQL's NaN handling policy where NaN is considered greater than all regular numbers
- Part of PostgreSQL's arithmetic data type operators system
- Used internally by the SQL parser and executor when processing float4 greater-than-or-equal-to operations

## Simplified Source

```c
Datum float4ge(PG_FUNCTION_ARGS) {
    // Extract the two float4 arguments from SQL call
    float4 arg1 = PG_GETARG_FLOAT4(0);
    float4 arg2 = PG_GETARG_FLOAT4(1);

    // Delegate to helper function and return boolean result
    return PG_RETURN_BOOL(float4_ge(arg1, arg2));
}
```