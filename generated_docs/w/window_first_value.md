# window_first_value

## Location
[src/backend/utils/adt/windowfuncs.c:649-669](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/windowfuncs.c#L649-L669)

## Overview
Implements the SQL window function FIRST_VALUE(), which returns the value of the specified expression evaluated at the first row of the window frame.

## Definition


## Detailed Description
The  function is the backend implementation of PostgreSQL's FIRST_VALUE() window function. It retrieves and returns the value of the first argument (the value expression) evaluated on the first row within the current window frame. This function follows the SQL standard specification for window functions.

The function operates by:
1. Obtaining the current window object context
2. Using  to fetch the argument value from the head (first row) of the window frame
3. Returning the retrieved value or NULL if the value is null

## Parameters / Member Variables
This function uses the standard PostgreSQL function call interface:
- Uses  macro for function arguments
- The first argument (arg 0) is the value expression to be evaluated

## Dependencies
- Functions called/Symbols referenced:
  - : Macro to get the current window object
  - : Core function to retrieve argument values from window frame
  - : Constant specifying to seek from the head (first row)
  - : Macro to return a Datum value
  - : Macro to return NULL
- Called from (representative examples):
  - No direct references found (typically called through the function manager)

## Notes and Other Information
- This function is part of the window function infrastructure in PostgreSQL
- It's registered in the system catalog and called indirectly through the function manager when FIRST_VALUE() is used in SQL
- The function handles NULL values appropriately by checking the  parameter
- Located in 
- Uses the frame-based approach rather than partition-based, meaning it respects the ROWS/RANGE frame specification