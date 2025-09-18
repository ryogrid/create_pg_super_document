# inetpl

## Location
src/backend/utils/adt/network.c: 1998 - 2007

## Overview
A PostgreSQL built-in function that adds a 64-bit integer to an inet address, providing IP address arithmetic functionality for SQL operations.

## Definition
Datum inetpl(PG_FUNCTION_ARGS)

## Detailed Description
The `inetpl` function serves as the PostgreSQL SQL-callable interface for adding a 64-bit integer to an inet address. It extracts the inet address and integer addend from the function arguments using PostgreSQL's function call macros, delegates the actual arithmetic computation to the `internal_inetpl` helper function, and returns the result in the proper PostgreSQL Datum format.

This function enables SQL expressions like `inet_address + integer_value` to perform IP address arithmetic operations within PostgreSQL queries.

## Parameters / Member Variables
- Function uses PostgreSQL's `PG_FUNCTION_ARGS` convention:
  - Argument 0: inet address (accessed via `PG_GETARG_INET_PP`)
  - Argument 1: 64-bit integer addend (accessed via `PG_GETARG_INT64`)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INET_PP
  - PG_GETARG_INT64  
  - internal_inetpl
  - PG_RETURN_INET_P
- Called from (representative examples):
  - SQL queries using inet + bigint operator

## Notes and Other Information
- This is a thin wrapper around `internal_inetpl` that handles PostgreSQL function call protocol
- Supports the SQL `+` operator for inet and bigint data types
- Part of PostgreSQL's network data type operator implementations
- Function follows PostgreSQL's V1 calling convention using standard macros
- Returns the result as a PostgreSQL inet Datum for SQL consumption