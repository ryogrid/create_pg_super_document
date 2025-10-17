# nameiclike

## Location
[src/backend/utils/adt/like.c:370-384](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/like.c#L370-L384)

## Overview
A PostgreSQL function that performs case-insensitive LIKE pattern matching on Name data types using the ILIKE operator.

## Definition

```c
Datum
nameiclike(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function implements case-insensitive pattern matching for PostgreSQL's Name data type. It takes a Name value and a text pattern as input, converts the Name to text format, and then uses the generic case-insensitive text matching function  to perform the pattern matching operation. This function is the backend implementation for the ILIKE operator when applied to Name data types.

The function follows PostgreSQL's standard function call convention using  and returns a Datum containing a boolean result indicating whether the Name matches the pattern in a case-insensitive manner.

## Parameters / Member Variables
- : The Name value to be matched against the pattern
- : The text pattern to match against (supports SQL LIKE wildcards % and _)

## Dependencies
- Functions called/Symbols referenced:
  -  - Extract Name argument from function call
  -  - Extract text pattern argument 
  -  - Convert Datum to text pointer
  -  - Direct function call interface
  -  - Convert Name to text format
  -  - Convert Name to Datum
  -  - Perform case-insensitive pattern matching
  -  - Get collation information for the operation
  -  - Constant representing successful match
  -  - Return boolean result
- Called from: 
  - This function is called through PostgreSQL's function manager when the ILIKE operator is used with Name data types

## Notes and Other Information
- This function is part of PostgreSQL's LIKE/ILIKE operator implementation
- It specifically handles the case-insensitive variant (ILIKE) for Name data types
- The function converts Name to text before processing since the generic matching function works with text
- Located in src/backend/utils/adt/like.c:370-384
- Uses PostgreSQL's collation system for proper case-insensitive matching across different locales

## Simplified Source

```c
Datum
nameiclike(PG_FUNCTION_ARGS)
{
    // Extract arguments
    Name str = PG_GETARG_NAME(0);
    text *pat = PG_GETARG_TEXT_PP(1);

    // Convert Name to text for processing
    text *strtext = DatumGetTextPP(DirectFunctionCall1(name_text, NameGetDatum(str)));

    // Perform case-insensitive pattern matching
    bool result = (Generic_Text_IC_like(strtext, pat, PG_GET_COLLATION()) == LIKE_TRUE);

    PG_RETURN_BOOL(result);
}
```