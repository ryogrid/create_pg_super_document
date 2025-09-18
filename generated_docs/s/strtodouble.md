# strtodouble

## Location
src/bin/pgbench/pgbench.c: 1059 - 1087

## Overview
Converts a string to a double-precision floating-point number with overflow/underflow detection and comprehensive error handling for pgbench.

## Definition


## Detailed Description
The  function provides a robust string-to-double conversion mechanism specifically designed for pgbench operations. It wraps the standard  function with additional error checking to detect numeric overflows, underflows, and invalid input syntax. The function supports both error-reporting and silent-error modes, making it suitable for different contexts where string-to-double conversion is needed with varying error handling requirements.

The function uses  to detect range errors from  and performs additional validation to ensure the entire string represents a valid number. It provides detailed error messages when operating in non-silent mode.

## Parameters / Member Variables
- : Input string containing the numeric value to be converted
- : Boolean flag controlling error reporting behavior - if false, errors are logged; if true, errors are returned silently  
- : Pointer to double variable where the converted result will be stored

## Dependencies
- Functions called/Symbols referenced:
  - strtod (standard C library function)
  - pg_log_error (PostgreSQL logging function)
  - unlikely (PostgreSQL optimization macro)
- Called from (representative examples):
  - makeVariableValue (at src/bin/pgbench/pgbench.c:1713)

## Notes and Other Information
- Returns true on successful conversion, false on any error condition
- Uses  to detect numeric range errors (overflow/underflow)
- Validates that the entire string is consumed during conversion to prevent partial parsing
- Part of pgbench utility for PostgreSQL performance testing
- The  macro is used for branch prediction optimization, indicating error conditions are expected to be rare
- Error messages follow PostgreSQL's standard logging format for consistency with other pgbench operations