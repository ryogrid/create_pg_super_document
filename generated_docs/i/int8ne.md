# int8ne

## Location
src/backend/utils/adt/int8.c: 122 - 130

## Overview
PostgreSQL function that compares two 64-bit integers and returns true if they are not equal.

## Definition


## Detailed Description
The  function implements the "not equal" comparison operator (<>) for the PostgreSQL bigint (int8) data type. It takes two 64-bit signed integers as arguments through the PostgreSQL function call interface and performs a simple inequality comparison. The function is part of PostgreSQL's type system infrastructure, providing the underlying implementation for SQL expressions like .

This function follows PostgreSQL's standard function calling conventions using the  macro to access arguments and  to return the boolean result.

## Parameters / Member Variables
- Function uses  calling convention:
  - Argument 0: First 64-bit integer value ()
  - Argument 1: Second 64-bit integer value ()

## Dependencies
- Functions called/Symbols referenced:
  - : Macro to extract 64-bit integer arguments from function call
  - : Macro to return boolean result from PostgreSQL function
- Called from (representative examples):
  - SQL engine when evaluating bigint <> operations
  - Internal comparison operations in PostgreSQL

## Notes and Other Information
- Located in 
- Part of the int8 (bigint) data type implementation
- Simple wrapper around C's  operator with PostgreSQL function interface
- Used internally by PostgreSQL's SQL engine for bigint inequality comparisons
- No overflow or error checking needed as this is a simple comparison operation