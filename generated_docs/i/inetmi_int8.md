# inetmi_int8

## Location
src/backend/utils/adt/network.c: 2008 - 2017

## Overview
A PostgreSQL built-in function that subtracts a 64-bit integer from an inet address, providing IP address subtraction functionality for SQL operations.

## Definition
Datum inetmi_int8(PG_FUNCTION_ARGS)

## Detailed Description
The `inetmi_int8` function serves as the PostgreSQL SQL-callable interface for subtracting a 64-bit integer from an inet address. It extracts the inet address and integer subtrahend from the function arguments using PostgreSQL's function call macros, then cleverly delegates to the existing `internal_inetpl` helper function by negating the subtrahend to perform addition with a negative value.

This function enables SQL expressions like `inet_address - integer_value` to perform IP address arithmetic operations within PostgreSQL queries. The implementation demonstrates code reuse by leveraging the addition logic with a negated operand rather than duplicating the complex arithmetic and overflow checking logic.

## Parameters / Member Variables
- Function uses PostgreSQL's `PG_FUNCTION_ARGS` convention:
  - Argument 0: inet address (accessed via `PG_GETARG_INET_PP`)
  - Argument 1: 64-bit integer subtrahend (accessed via `PG_GETARG_INT64`)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INET_PP
  - PG_GETARG_INT64
  - [internal_inetpl](internal_inetpl.md)
  - PG_RETURN_INET_P
- Called from (representative examples):
  - SQL queries using inet - bigint operator

## Notes and Other Information
- Implements subtraction by calling `internal_inetpl` with negated addend (`-addend`)
- This approach reuses the existing addition logic and overflow detection mechanisms
- Supports the SQL `-` operator for inet and bigint data types
- Part of PostgreSQL's network data type operator implementations
- Function follows PostgreSQL's V1 calling convention using standard macros
- More efficient than implementing separate subtraction logic due to code reuse