# pg_num_nonnulls

## Location
src/backend/utils/adt/misc.c: 178 - 194

## Overview
A PostgreSQL built-in function that counts and returns the number of non-NULL arguments passed to it, supporting both individual arguments and variadic arrays.

## Definition


## Detailed Description
The pg_num_nonnulls function provides a way to count non-NULL values among a set of arguments in PostgreSQL. It uses the same count_nulls helper function as pg_num_nulls but returns the complement - the number of arguments that are NOT NULL.

The function calculates the result by subtracting the null count from the total argument count (nargs - nulls). Like its sibling function pg_num_nulls, it can handle both individual arguments and variadic arrays, and returns NULL when meaningful analysis cannot be performed (such as when a variadic array argument itself is NULL).

This function is useful in SQL queries for data completeness analysis, validation logic, and statistical operations where the count of valid (non-null) values is needed.

## Parameters / Member Variables
- Uses the standard PostgreSQL function call interface 
- Accepts a variable number of arguments of any type through PostgreSQL's variadic function mechanism

## Dependencies
- Functions called/Symbols referenced:
  - count_nulls
  - PG_RETURN_NULL
  - PG_RETURN_INT32
- Called from (representative examples):
  - SQL queries and user-defined functions
  - No direct C code references found in the analyzed codebase

## Notes and Other Information
- This function is part of PostgreSQL's standard SQL function library
- Returns the SQL function name 'num_nonnulls' when called from SQL
- The function signature uses PostgreSQL's standard Datum return type for SQL-callable functions
- Follows PostgreSQL's convention of returning NULL when meaningful analysis cannot be performed
- Can be used with both fixed argument lists and variadic argument patterns in SQL
- Mathematically equivalent to (total_args - null_count) where both values come from the count_nulls helper function