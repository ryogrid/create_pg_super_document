# texticnlike

## Location
src/backend/utils/adt/like.c: 412 - 427

## Overview
A PostgreSQL function that performs case-insensitive NOT LIKE pattern matching on text data types using the NOT ILIKE operator.

## Definition
```c
Datum texticnlike(PG_FUNCTION_ARGS)
```

## Detailed Description
The `texticnlike` function implements case-insensitive negative pattern matching for PostgreSQL's text data type. It takes two text arguments - the string to be matched and the pattern - and uses the generic case-insensitive text matching function `Generic_Text_IC_like` to perform the pattern matching operation, then negates the result. This function is the backend implementation for the NOT ILIKE operator when applied to text data types.

Like `texticlike`, this function works directly with text arguments without needing data type conversion. The key difference is that it returns true when the text does NOT match the pattern in a case-insensitive manner.

## Parameters / Member Variables
- `PG_GETARG_TEXT_PP(0)`: The text string to be matched against the pattern
- `PG_GETARG_TEXT_PP(1)`: The text pattern to match against (supports SQL LIKE wildcards % and _)

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_TEXT_PP` - Extract text arguments from function call
  - `Generic_Text_IC_like` - Perform case-insensitive pattern matching
  - `PG_GET_COLLATION` - Get collation information for the operation
  - `LIKE_TRUE` - Constant representing successful match
  - `PG_RETURN_BOOL` - Return boolean result
- Called from: 
  - This function is called through PostgreSQL's function manager when the NOT ILIKE operator is used with text data types

## Notes and Other Information
- This is the negated version of `texticlike`
- The key difference is the != comparison instead of == when checking against LIKE_TRUE
- No data type conversion is needed since both inputs are already text
- Part of PostgreSQL's LIKE/ILIKE operator implementation for negative text matching
- Located in src/backend/utils/adt/like.c:412-427
- More efficient than Name variants due to lack of conversion overhead
- Uses PostgreSQL's collation system for proper case-insensitive matching across different locales