# texticlike

## Location
src/backend/utils/adt/like.c: 400 - 411

## Overview
A PostgreSQL function that performs case-insensitive LIKE pattern matching on text data types using the ILIKE operator.

## Definition
```c
Datum texticlike(PG_FUNCTION_ARGS)
```

## Detailed Description
The `texticlike` function implements case-insensitive pattern matching for PostgreSQL's text data type. It takes two text arguments - the string to be matched and the pattern - and directly uses the generic case-insensitive text matching function `Generic_Text_IC_like` to perform the pattern matching operation. This function is the backend implementation for the ILIKE operator when applied to text data types.

Unlike the Name variants (`nameiclike`), this function works directly with text arguments without needing any data type conversion, making it more straightforward and efficient for text-to-text matching operations.

## Parameters / Member Variables
- `PG_GETARG_TEXT_PP(0)`: The text string to be matched against the pattern
- `PG_GETARG_TEXT_PP(1)`: The text pattern to match against (supports SQL LIKE wildcards % and _)

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_TEXT_PP` - Extract text arguments from function call
  - [Generic_Text_IC_like](../G/Generic_Text_IC_like.md) - Perform case-insensitive pattern matching
  - `PG_GET_COLLATION` - Get collation information for the operation
  - `LIKE_TRUE` - Constant representing successful match
  - `PG_RETURN_BOOL` - Return boolean result
- Called from: 
  - This function is called through PostgreSQL's function manager when the ILIKE operator is used with text data types

## Notes and Other Information
- This is the most direct implementation of case-insensitive LIKE matching in PostgreSQL
- No data type conversion is needed since both inputs are already text
- Part of PostgreSQL's LIKE/ILIKE operator implementation for text types
- Located in src/backend/utils/adt/like.c:400-411
- More efficient than Name variants due to lack of conversion overhead
- Uses PostgreSQL's collation system for proper case-insensitive matching across different locales