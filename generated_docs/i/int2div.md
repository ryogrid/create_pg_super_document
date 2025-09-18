# int2div

## Location
src/backend/utils/adt/int.c: 949 - 987

## Overview
A PostgreSQL system function that implements division for 16-bit signed integers (int2/smallint) with comprehensive error checking for division by zero and overflow conditions.

## Definition
```c
Datum int2div(PG_FUNCTION_ARGS)
```

## Detailed Description
The int2div function implements the binary division operator (/) for PostgreSQL's int2 (smallint) data type. It performs integer division with careful handling of edge cases that could cause errors or undefined behavior. The function includes explicit checks for division by zero and handles the special case of dividing the minimum int16 value (-32768) by -1, which would cause overflow since the mathematical result (32768) cannot be represented in int16.

The function handles two critical edge cases: (1) division by zero, which raises a DIVISION_BY_ZERO error, and (2) the overflow condition PG_INT16_MIN / -1, which would mathematically result in 32768 but cannot be represented in int16 range (-32768 to 32767), raising a NUMERIC_VALUE_OUT_OF_RANGE error.

## Parameters / Member Variables
- `arg1`: First int16 operand (dividend) obtained via PG_GETARG_INT16(0)
- `arg2`: Second int16 operand (divisor) obtained via PG_GETARG_INT16(1)  
- `result`: Local variable to store the division result

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT16 (macro for extracting int16 arguments)
  - PG_INT16_MIN (constant representing minimum int16 value)
  - ereport (error reporting function)
  - [errcode](../e/errcode.md)/errmsg (error handling macros)
  - PG_RETURN_INT16 (macro for returning int16 value)
  - PG_RETURN_NULL (macro for returning null, used after error for compiler)
- Called from (representative examples):
  - No direct references found in the codebase (likely called through operator dispatch)

## Notes and Other Information
- Located in src/backend/utils/adt/int.c:949-987
- This function is typically invoked through PostgreSQL's operator system when the / operator is used with two smallint values
- Throws ERRCODE_DIVISION_BY_ZERO error when divisor is zero
- Throws ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE error for the specific overflow case PG_INT16_MIN / -1
- Part of PostgreSQL's arithmetic operator family for the int2/smallint data type
- Uses the unlikely() macro hint to optimize for the rare overflow case
- The PG_RETURN_NULL() after the division by zero error is included as a compiler hint to prevent spurious warnings, but will never actually execute since ereport with ERROR level does not return
- For all other division operations, no overflow is possible and the standard division is performed safely