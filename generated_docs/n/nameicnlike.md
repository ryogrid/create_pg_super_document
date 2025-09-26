# nameicnlike

## Location
[src/backend/utils/adt/like.c:385-399](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/like.c#L385-L399)

## Overview
A PostgreSQL function that performs case-insensitive NOT LIKE pattern matching on Name data types using the NOT ILIKE operator.

## Definition
```c
Datum nameicnlike(PG_FUNCTION_ARGS)
```

## Detailed Description
The `nameicnlike` function implements case-insensitive negative pattern matching for PostgreSQL's Name data type. It takes a Name value and a text pattern as input, converts the Name to text format, and then uses the generic case-insensitive text matching function `Generic_Text_IC_like` to perform the pattern matching operation. The key difference from `nameiclike` is that it negates the result, returning true when the Name does NOT match the pattern in a case-insensitive manner.

This function is the backend implementation for the NOT ILIKE operator when applied to Name data types. It follows the same conversion and matching logic as `nameiclike` but inverts the boolean result.

## Parameters / Member Variables
- `PG_GETARG_NAME(0)`: The Name value to be matched against the pattern
- `PG_GETARG_TEXT_PP(1)`: The text pattern to match against (supports SQL LIKE wildcards % and _)

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_NAME` - Extract Name argument from function call
  - `PG_GETARG_TEXT_PP` - Extract text pattern argument 
  - `DatumGetTextPP` - Convert Datum to text pointer
  - `DirectFunctionCall1` - Direct function call interface
  - `[name_text](name_text.md)` - Convert Name to text format
  - [NameGetDatum](../N/NameGetDatum.md) - Convert Name to Datum
  - [Generic_Text_IC_like](../G/Generic_Text_IC_like.md) - Perform case-insensitive pattern matching
  - `PG_GET_COLLATION` - Get collation information for the operation
  - `LIKE_TRUE` - Constant representing successful match
  - `PG_RETURN_BOOL` - Return boolean result
- Called from: 
  - This function is called through PostgreSQL's function manager when the NOT ILIKE operator is used with Name data types

## Notes and Other Information
- This function is the negated version of `nameiclike`
- The key difference is the != comparison instead of == when checking against LIKE_TRUE
- Part of PostgreSQL's LIKE/ILIKE operator implementation for negative matching
- Converts Name to text before processing since the generic matching function works with text
- Located in src/backend/utils/adt/like.c:385-399
- Uses PostgreSQL's collation system for proper case-insensitive matching across different locales