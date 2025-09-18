# point_in

## Location
[src/backend/utils/adt/geo_ops.c:1831-1841](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L1831-L1841)

## Overview
Converts a string representation of a 2D point into PostgreSQL's internal Point data structure.

## Definition


## Detailed Description
The  function is responsible for parsing string input and converting it into PostgreSQL's internal Point data type. It accepts two external string formats:
- "(x,y)" - parenthesized format
- "x,y" - simple comma-separated format

The function allocates memory for a new Point structure and uses the  utility function to parse the x and y coordinates from the input string. Error handling is delegated to , which will report parsing errors appropriately.

## Parameters / Member Variables
- : Input string containing the point representation to be parsed

## Dependencies
- Functions called/Symbols referenced:
  -  - PostgreSQL's 2D point data structure
  -  - Utility function for parsing coordinate pairs from strings
  -  - Macro for returning Point data from PostgreSQL functions
- Called from (representative examples):
  - No direct references found (likely called through PostgreSQL's type system)

## Notes and Other Information
- This function is part of PostgreSQL's geometric data type system
- Memory allocation is performed using  which integrates with PostgreSQL's memory management
- The function ignores return values from  as error handling is managed through PostgreSQL's error context system
- This is an input function for the Point data type, typically registered in the PostgreSQL type system catalog