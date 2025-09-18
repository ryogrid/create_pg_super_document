# textgtname

## Location
src/backend/utils/adt/varlena.c: 2774 - 2779

## Overview
PostgreSQL function that compares a text value with a name value and returns true if the text is greater than the name in lexicographic order.

## Definition


## Detailed Description
The  function is a PostgreSQL built-in function that performs a comparison between a text value and a name value, returning a boolean result indicating whether the text value is lexicographically greater than the name value. This function is part of PostgreSQL's text comparison operators and is typically used in SQL queries with the  operator when comparing text and name types.

The function uses the  macro with the  comparison function to perform the actual comparison logic, returning true only when the comparison result is greater than 0.

## Parameters / Member Variables
- Uses  macro to access function arguments:
  - First argument: text value (implicitly accessed via PG_GETARG macros)
  - Second argument: name value (implicitly accessed via PG_GETARG macros)

## Dependencies
- Functions called/Symbols referenced:
  -  - Macro for calling comparison functions
  -  - B-tree comparison function for text vs name
- Called from (representative examples):
  - No direct references found in the codebase (likely called via SQL operator framework)

## Notes and Other Information
- This function is part of PostgreSQL's operator framework and is typically invoked through SQL comparison operators rather than direct function calls
- The function is defined in  at lines 2774-2779
- Returns a Datum containing a boolean value using the  macro
- The actual comparison logic is delegated to  which handles the type-specific comparison between text and name types