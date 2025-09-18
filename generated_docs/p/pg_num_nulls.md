# pg_num_nulls

## Location
[src/backend/utils/adt/misc.c:162-177](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/misc.c#L162-L177)

## Overview
A PostgreSQL built-in function that counts and returns the number of NULL arguments passed to it, supporting both individual arguments and variadic arrays.

## Definition


## Detailed Description
The pg_num_nulls function provides a way to count NULL values among a set of arguments in PostgreSQL. It leverages the count_nulls helper function to handle the complexity of processing both individual arguments and variadic arrays. The function is exposed to SQL users through the PostgreSQL function interface.

When called, it examines all provided arguments (whether passed individually or as a variadic array) and returns an integer count of how many are NULL. If the function cannot determine a meaningful result (such as when a variadic array argument itself is NULL), it returns NULL.

This function is commonly used in SQL queries where conditional logic based on null counts is needed, data validation scenarios, or statistical analysis of missing values.

## Parameters / Member Variables
- Uses the standard PostgreSQL function call interface 
- Accepts a variable number of arguments of any type through PostgreSQL's variadic function mechanism

## Dependencies
- Functions called/Symbols referenced:
  - [count_nulls](../c/count_nulls.md)
  - PG_RETURN_NULL
  - PG_RETURN_INT32
- Called from (representative examples):
  - SQL queries and user-defined functions
  - No direct C code references found in the analyzed codebase

## Notes and Other Information
- This function is part of PostgreSQL's standard SQL function library
- Returns the SQL function name 'num_nulls' when called from SQL
- The function signature uses PostgreSQL's standard Datum return type for SQL-callable functions
- Follows PostgreSQL's convention of returning NULL when meaningful analysis cannot be performed
- Can be used with both fixed argument lists and variadic argument patterns in SQL