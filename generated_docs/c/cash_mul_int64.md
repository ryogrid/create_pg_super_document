# cash_mul_int64

## Location
[src/backend/utils/adt/cash.c:143-155](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/cash.c#L143-L155)

## Overview
An inline static helper function that performs safe multiplication of a Cash value with a 64-bit integer, providing overflow detection and error reporting.

## Definition


## Detailed Description
This function multiplies a Cash value by a 64-bit signed integer while checking for arithmetic overflow. It uses PostgreSQL's safe multiplication utility function pg_mul_s64_overflow to detect potential overflow conditions. If an overflow would occur, it reports an error with the message "money out of range" using PostgreSQL's error reporting system. The function is designed to be inlined for performance optimization in cash arithmetic operations.

## Parameters / Member Variables
- `c`: The Cash value to be multiplied (input operand)
- `i`: The 64-bit signed integer multiplier

## Dependencies
- Functions called/Symbols referenced:
  - Cash (data type)
  - [pg_mul_s64_overflow](../p/pg_mul_s64_overflow.md) (overflow detection utility)
  - ereport (error reporting function)
  - unlikely (branch prediction hint macro)
- Called from (representative examples):
  - [cash_mul_int8](cash_mul_int8.md)
  - [int8_mul_cash](../i/int8_mul_cash.md)
  - [cash_mul_int4](cash_mul_int4.md)
  - [int4_mul_cash](../i/int4_mul_cash.md)
  - [cash_mul_int2](cash_mul_int2.md)
  - [int2_mul_cash](../i/int2_mul_cash.md)

## Notes and Other Information
- This is a static inline function, meaning it's only accessible within the same compilation unit and is likely to be inlined at call sites
- Uses PostgreSQL's overflow-safe arithmetic to prevent silent integer overflow bugs
- Part of the PostgreSQL money/cash data type implementation
- The function throws a NUMERIC_VALUE_OUT_OF_RANGE error when overflow is detected
- Used as a building block for various cash multiplication operations with different integer types