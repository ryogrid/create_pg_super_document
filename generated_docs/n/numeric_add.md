# numeric_add

## Location
src/backend/utils/adt/numeric.c: 2864 - 2882

## Overview
Performs addition of two PostgreSQL numeric values, providing the standard SQL + operator functionality for arbitrary precision arithmetic.

## Definition


## Detailed Description
This function implements the addition operation for PostgreSQL's numeric data type, which supports arbitrary precision decimal arithmetic. It serves as the SQL-level interface for numeric addition, handling all the complexity of multi-precision arithmetic through delegation to the internal numeric_add_opt_error function.

The function is designed as a thin wrapper that extracts the two numeric operands from the function arguments and calls the internal implementation with no error checking options (NULL error flag), meaning any arithmetic errors will be reported via PostgreSQL's standard error mechanism.

## Parameters / Member Variables
-  (PG_GETARG_NUMERIC(0)): The first numeric operand
-  (PG_GETARG_NUMERIC(1)): The second numeric operand

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_NUMERIC (parameter extraction)
  - numeric_add_opt_error (core addition implementation)
  - PG_RETURN_NUMERIC (result return)
- Called from (representative examples):
  - numeric_half_rounded (database size calculations)
  - int8_sum (bigint sum aggregation)
  - pg_lsn_pli (LSN arithmetic operations)

## Notes and Other Information
- This is the primary entry point for SQL-level numeric addition operations
- Delegates the actual arithmetic to numeric_add_opt_error for implementation
- Handles all standard numeric addition cases including NaN and infinity
- Used extensively throughout PostgreSQL for precise decimal arithmetic
- Forms part of PostgreSQL's comprehensive arbitrary precision numeric system
- Errors in arithmetic (like overflow) are handled via standard PostgreSQL error reporting
- Essential for financial calculations and any operations requiring exact decimal arithmetic