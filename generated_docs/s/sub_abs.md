# sub_abs

## Location
[src/backend/utils/adt/numeric.c:11685-11766](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L11685-L11766)

## Overview
Subtracts the absolute value of var2 from the absolute value of var1 and stores the result in the result variable, used as a core arithmetic operation in PostgreSQL's numeric type implementation.

## Definition

```c
static void
sub_abs(const NumericVar *var1, const NumericVar *var2, NumericVar *result)
```
## Detailed Description
The  function performs subtraction of absolute values between two NumericVar operands. It implements multi-precision arithmetic by working with digit arrays in base NBASE representation. The function requires that the absolute value of var1 must be greater than or equal to the absolute value of var2 to ensure the result is non-negative. The operation handles borrowing between digits and properly manages the weight and scale of the result. The result can safely point to one of the operands without causing memory corruption.

## Parameters / Member Variables
- : Pointer to the first NumericVar operand (minuend) - must have absolute value >= |var2|
- : Pointer to the second NumericVar operand (subtrahend) to be subtracted
- : Pointer to NumericVar structure where the result ABS(var1) - ABS(var2) will be stored

## Dependencies
- Functions called/Symbols referenced:
  - NumericDigit (type for individual digits)
  - digitbuf_alloc (allocates digit buffer)
  - digitbuf_free (frees digit buffer)
  - [strip_var](strip_var.md) (removes leading/trailing zeros)
  - NBASE (numeric base constant)
- Called from (representative examples):
  - [add_var](../a/add_var.md) (addition operations)
  - [sub_var](sub_var.md) (subtraction operations)
  - [PGTYPESnumeric_add](../P/PGTYPESnumeric_add.md) (ECPG numeric addition)
  - [PGTYPESnumeric_sub](../P/PGTYPESnumeric_sub.md) (ECPG numeric subtraction)
  - [PGTYPESnumeric_div](../P/PGTYPESnumeric_div.md) (ECPG numeric division)

## Notes and Other Information
- Critical precondition: ABS(var1) MUST BE GREATER OR EQUAL ABS(var2) - violation will cause assertion failure
- The function works with digit-by-digit subtraction using borrowing mechanism
- Manages decimal scale (dscale) and weight properly for accurate decimal arithmetic
- Uses local variable copies for performance optimization in the inner loop
- Automatically strips leading and trailing zeros from the result
- Memory-safe: result parameter can alias with either input operand