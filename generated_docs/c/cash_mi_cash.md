# cash_mi_cash

## Location
src/backend/utils/adt/cash.c: 104 - 116

## Overview
A private inline function that performs safe subtraction of two Cash values with overflow detection and error reporting.

## Definition


## Detailed Description
The  function provides safe subtraction of two Cash values (64-bit signed integers representing monetary amounts). It uses PostgreSQL's overflow-safe arithmetic function  to detect integer underflow conditions that could occur during subtraction. If an overflow/underflow would occur, the function reports a "money out of range" error using PostgreSQL's error reporting mechanism. This ensures that monetary calculations remain within valid bounds and prevents silent arithmetic errors that could corrupt financial data.

## Parameters / Member Variables
- : Cash value to subtract from (minuend)
- : Cash value to subtract (subtrahend)

## Dependencies
- Functions called/Symbols referenced:
  - Cash (type)
  - pg_sub_s64_overflow (overflow-safe subtraction)
  - ereport (error reporting)
  - errcode (error code specification)
  - errmsg (error message formatting)
  - ERROR (error level constant)
  - ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE (specific error code)
- Called from (representative examples):
  - cash_mi

## Notes and Other Information
- Declared as  for performance optimization in arithmetic operations
- Uses PostgreSQL's safe arithmetic functions to prevent integer underflow/overflow
- Part of PostgreSQL's cash data type implementation for monetary arithmetic
- The error message "money out of range" provides clear indication of overflow/underflow conditions
- Returns the difference result (c1 - c2) only if no overflow occurs, otherwise throws an error
- Complementary function to  for subtraction operations