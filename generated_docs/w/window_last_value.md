# window_last_value

## Location
[src/backend/utils/adt/windowfuncs.c:670-690](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/windowfuncs.c#L670-L690)

## Overview
Implements the SQL window function LAST_VALUE(), which returns the value of the specified expression evaluated at the last row of the window frame.

## Definition

```c
Datum
window_last_value(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is the backend implementation of PostgreSQL's LAST_VALUE() window function. It retrieves and returns the value of the first argument (the value expression) evaluated on the last row within the current window frame. This function follows the SQL standard specification for window functions.

The function operates by:
1. Obtaining the current window object context
2. Using  to fetch the argument value from the tail (last row) of the window frame
3. Returning the retrieved value or NULL if the value is null

The key difference from  is the use of  to access the last row instead of the first row.

## Parameters / Member Variables
This function uses the standard PostgreSQL function call interface:
- Uses  macro for function arguments
- The first argument (arg 0) is the value expression to be evaluated

## Dependencies
- Functions called/Symbols referenced:
  - : Macro to get the current window object
  - : Core function to retrieve argument values from window frame
  - : Constant specifying to seek from the tail (last row)
  - : Macro to return a Datum value
  - : Macro to return NULL
- Called from (representative examples):
  - No direct references found (typically called through the function manager)

## Notes and Other Information
- This function is part of the window function infrastructure in PostgreSQL
- It's registered in the system catalog and called indirectly through the function manager when LAST_VALUE() is used in SQL
- The function handles NULL values appropriately by checking the  parameter
- Located in
- Uses the frame-based approach rather than partition-based, meaning it respects the ROWS/RANGE frame specification
- Commonly used with unbounded preceding and current row frame specification to get meaningful results

## Simplified Source

```c
Datum
window_last_value(PG_FUNCTION_ARGS)
{
    WindowObject winobj = PG_WINDOW_OBJECT();
    Datum result;
    bool is_null;

    // Get the value from the last row of the window frame
    result = WinGetFuncArgInFrame(winobj, 0, 0, WINDOW_SEEK_TAIL,
                                  true, &is_null, NULL);

    if (is_null)
        PG_RETURN_NULL();

    PG_RETURN_DATUM(result);
}
```