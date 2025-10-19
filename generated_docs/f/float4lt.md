# float4lt

## Location
[src/backend/utils/adt/float.c:837-845](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/float.c#L837-L845)

## Overview
PostgreSQL SQL-callable function that tests whether one single-precision floating-point number is less than another, handling NaN values according to IEEE floating-point standards.

## Definition

```c
Datum
float4lt(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is a PostgreSQL built-in function that implements the less-than comparison operator () for single-precision floating-point numbers (float4 type). It extracts two float4 arguments from the function call context and delegates the actual comparison logic to the  inline helper function.

The function properly handles IEEE 754 floating-point special cases, particularly NaN (Not-a-Number) values. According to IEEE standards, any comparison involving NaN returns false, except for not-equal comparisons. However, PostgreSQL's implementation treats NaN as greater than any regular number for ordering purposes.

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
-  returns true only if the first argument is not NaN AND either the second argument is NaN or the first is numerically less than the second
- This implements PostgreSQL's NaN handling policy where NaN is considered greater than all regular numbers
- Part of PostgreSQL's arithmetic data type operators system
- Used internally by the SQL parser and executor when processing float4 less-than operations

## Simplified Source

```c
Datum
float4lt(PG_FUNCTION_ARGS)
{
    // Extract two float4 arguments from function call
    float4 arg1 = PG_GETARG_FLOAT4(0);
    float4 arg2 = PG_GETARG_FLOAT4(1);

    // Perform NaN-aware less-than comparison
    // PostgreSQL treats NaN as greater than all regular numbers
    // Returns true only if:
    // - arg1 is not NaN AND
    // - (arg2 is NaN OR arg1 < arg2)
    bool result = !isnan(arg1) && (isnan(arg2) || arg1 < arg2);

    PG_RETURN_BOOL(result);
}
```