# i4tof

## Location
[src/backend/utils/adt/float.c:1331-1342](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/float.c#L1331-L1342)

## Overview
Converts a 32-bit integer (int4) to a single-precision floating-point number (float4).

## Definition

```c
Datum
i4tof(PG_FUNCTION_ARGS)
```
## Detailed Description
This function implements the PostgreSQL built-in function for converting int4 (32-bit integer) values to float4 (single-precision floating-point) values. It follows the standard PostgreSQL function calling convention using the fmgr interface, taking arguments via PG_FUNCTION_ARGS and returning a Datum. The conversion is performed using a simple C cast from int32 to float4.

## Parameters / Member Variables
- Takes one argument accessed via : The int4 value to be converted to float4

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT32 (macro for retrieving int32 argument)
  - PG_RETURN_FLOAT4 (macro for returning float4 result)
  - float4 (typedef for single-precision float)

- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- Located in src/backend/utils/adt/float.c:1331-1342
- This is a straightforward type conversion function that relies on C's built-in casting
- The conversion may result in loss of precision for very large integer values that exceed the precision limits of single-precision floating-point representation
- Part of PostgreSQL's type conversion system for numeric types