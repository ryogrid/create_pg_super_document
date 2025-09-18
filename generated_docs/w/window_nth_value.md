# window_nth_value

## Location
src/backend/utils/adt/windowfuncs.c: 691 - 716

## Overview
Implements the SQL window function NTH_VALUE(), which returns the value of the specified expression evaluated at the nth row from the first row of the window frame.

## Definition


## Detailed Description
The  function is the backend implementation of PostgreSQL's NTH_VALUE() window function. It retrieves and returns the value of the first argument (the value expression) evaluated on the nth row within the current window frame, where n is specified by the second argument. This function follows the SQL standard specification for window functions.

The function operates by:
1. Obtaining the current window object context
2. Extracting the nth position from the second function argument
3. Validating that n is greater than zero (SQL standard requirement)
4. Using  to fetch the argument value from the nth row (n-1 in 0-based indexing)
5. Returning the retrieved value or NULL if the value is null or the row doesn't exist

## Parameters / Member Variables
This function uses the standard PostgreSQL function call interface:
- Uses  macro for function arguments
- The first argument (arg 0) is the value expression to be evaluated
- The second argument (arg 1) is the nth position (1-based) within the window frame
- Internal variables:
  - : The window object containing frame information
  - : Boolean indicating if the offset argument is constant
  - : The returned datum value
  - : Boolean indicating if the result is NULL
  - : The nth position extracted from the second argument

## Dependencies
- Functions called/Symbols referenced:
  - : Macro to get the current window object
  - : Function to get the current value of a function argument
  - : Function to extract int32 from a Datum
  - : Function to check if an argument expression is stable
  - : Core function to retrieve argument values from window frame
  - : Constant specifying to seek from the head (first row)
  - : Function to report errors
  - : Macro for error code handling
  - : Macro for error message handling
  - : Macro to return a Datum value
  - : Macro to return NULL
- Called from (representative examples):
  - No direct references found (typically called through the function manager)

## Notes and Other Information
- This function is part of the window function infrastructure in PostgreSQL
- It's registered in the system catalog and called indirectly through the function manager when NTH_VALUE() is used in SQL
- The function performs input validation, ensuring the nth argument is greater than zero
- Uses 1-based indexing for the SQL interface but converts to 0-based indexing internally (nth - 1)
- The  optimization allows for more efficient processing when the nth argument is constant
- Located in 
- Throws  error if nth <= 0
- Returns NULL if the nth row doesn't exist within the window frame or if the evaluated expression is NULL