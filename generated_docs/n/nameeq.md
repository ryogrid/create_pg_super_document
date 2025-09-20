# nameeq

## Location
[src/backend/utils/adt/name.c:148-156](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/name.c#L148-L156)

## Overview
The  function compares two PostgreSQL Name values for equality, returning true if they are equal according to the specified collation.

## Definition

```c
Datum
nameeq(PG_FUNCTION_ARGS)
```
## Detailed Description
 is a PostgreSQL built-in function that performs equality comparison between two Name data type values. It extracts two Name arguments from the function call context, compares them using the  function with the current collation setting, and returns a boolean result indicating whether the names are equal. This function is typically used in SQL WHERE clauses and JOIN conditions when comparing name-type columns.

## Parameters / Member Variables
- Uses  macro to access function arguments
- : First Name argument extracted using 
- : Second Name argument extracted using 

## Dependencies
- Functions called/Symbols referenced:
  -  - Macro to extract Name arguments
  -  - Core name comparison function
  -  - Macro to get current collation
  -  - Macro to return boolean result
- Called from (representative examples):
  - No direct references found in the codebase (likely called via SQL operator system)

## Notes and Other Information
- This function implements the equality operator (=) for the Name data type in PostgreSQL
- The function relies on  for the actual comparison logic, ensuring consistent collation handling
- Returns true (1) when names are equal, false (0) otherwise
- Part of PostgreSQL's type system infrastructure for the Name built-in type