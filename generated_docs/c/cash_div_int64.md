# cash_div_int64

## Location
src/backend/utils/adt/cash.c: 156 - 172

## Overview
An inline static helper function that performs safe division of a Cash value by a 64-bit integer, providing division-by-zero protection and error reporting.

## Definition
```c
static inline Cash cash_div_int64(Cash c, int64 i)
```

## Detailed Description
This function divides a Cash value by a 64-bit signed integer while checking for division by zero. It uses PostgreSQL's error reporting system to throw an appropriate error when division by zero is attempted. Unlike the multiplication counterpart, this function only needs to check for division by zero since integer division truncation is expected behavior for cash operations. The function is designed to be inlined for performance optimization in cash arithmetic operations.

## Parameters / Member Variables
- `c`: The Cash value to be divided (dividend)
- `i`: The 64-bit signed integer divisor

## Dependencies
- Functions called/Symbols referenced:
  - Cash (data type)
  - ereport (error reporting function)
  - unlikely (branch prediction hint macro)
- Called from (representative examples):
  - cash_div_int8
  - cash_div_int4
  - cash_div_int2

## Notes and Other Information
- This is a static inline function, meaning it's only accessible within the same compilation unit and is likely to be inlined at call sites
- Provides division-by-zero protection which is critical for financial calculations
- Part of the PostgreSQL money/cash data type implementation
- The function throws a DIVISION_BY_ZERO error when zero divisor is detected
- Used as a building block for various cash division operations with different integer types
- Performs integer division which truncates towards zero (standard C behavior)