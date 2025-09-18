# point_sub

## Location
src/backend/utils/adt/geo_ops.c: 4142 - 4156

## Overview
A PostgreSQL function that provides the SQL-callable interface for subtracting one geometric point from another.

## Definition


## Detailed Description
This function serves as the PostgreSQL SQL function interface for point subtraction operations. It extracts two Point arguments from the function call arguments using PostgreSQL's function calling convention, allocates memory for the result, delegates the actual computation to point_sub_point, and returns the result in PostgreSQL's Datum format. This function enables the '-' operator for point data types in SQL queries.

## Parameters / Member Variables
- Uses  macro to access function arguments:
  - Argument 0: First Point operand (minuend, accessed via )
  - Argument 1: Second Point operand (subtrahend, accessed via )

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_POINT_P
  - palloc
  - point_sub_point
  - PG_RETURN_POINT_P
  - Point (data type)
- Called from (representative examples):
  - SQL queries using point subtraction operator
  - PostgreSQL function call infrastructure

## Notes and Other Information
- This is a PostgreSQL internal function that implements the '-' operator for point data types
- Uses PostgreSQL's memory allocation (palloc) to ensure proper memory management within the database context
- Returns a Datum which is PostgreSQL's generic data type for SQL function returns
- The function signature follows PostgreSQL's V1 calling convention for internal functions
- Memory allocated with palloc will be automatically freed at the end of the current memory context
- Performs the operation result = p1 - p2 (order matters for subtraction)