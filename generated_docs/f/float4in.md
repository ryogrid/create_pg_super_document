# float4in

## Location
[src/backend/utils/adt/float.c:157-175](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/float.c#L157-L175)

## Overview
A PostgreSQL input function that converts string representations of floating-point numbers to single-precision float4 values, with special handling to avoid double-rounding precision errors.

## Definition

```c
Datum
float4in(PG_FUNCTION_ARGS)
```
## Detailed Description
This function serves as the input conversion routine for PostgreSQL's float4 (real) data type, converting string representations of numbers into internal float4 format. The function is notable for its careful handling of precision during conversion to avoid double-rounding errors that can occur when converting decimal strings to floating-point values.

The key innovation in this implementation is the use of  instead of  for the conversion. This prevents a double-rounding problem where:
1. A decimal input is first rounded to double precision
2. The double is then rounded to float precision
3. This two-step process can produce incorrect results compared to direct decimal-to-float conversion

The extensive comment in the source code provides a detailed mathematical example (7.038531e-26) demonstrating how this double-rounding can lead to incorrect results, with specific hexadecimal representations showing the precision differences.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: String representation of the floating-point number (extracted via )
## Dependencies
- Functions called/Symbols referenced:
  -  (macro to extract string argument)
  -  (internal conversion function)
  -  (macro to return float4 value)
  -  (function call context information)
- Called from (representative examples):
  -  - conversion from numeric to float4 type
  - PostgreSQL's type system for float4/real input operations

## Notes and Other Information
- This is a critical component of PostgreSQL's type system, specifically for the float4/real data type
- The function implements sophisticated floating-point conversion logic to maintain numerical accuracy
- The double-rounding problem addressed here is a subtle but important aspect of floating-point arithmetic
- Uses PostgreSQL's standard function calling conventions with  and return macros
- The detailed mathematical analysis in the comments demonstrates the precision considerations involved
- Part of PostgreSQL's comprehensive type input/output system
- The precision handling makes this function particularly important for scientific and financial applications where floating-point accuracy is critical