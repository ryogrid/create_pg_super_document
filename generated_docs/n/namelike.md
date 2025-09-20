# namelike

## Location
[src/backend/utils/adt/like.c:240-260](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/like.c#L240-L260)

## Overview
A PostgreSQL function that implements LIKE pattern matching for the Name data type, serving as the backend for the ~~ operator and LIKE expressions involving name columns.

## Definition

```c
Datum
namelike(PG_FUNCTION_ARGS)
```
## Detailed Description
The namelike function provides LIKE pattern matching functionality specifically for PostgreSQL's Name data type (used for object names like table names, column names, etc.). It acts as an interface between PostgreSQL's function call manager and the generic pattern matching infrastructure.

The function:
1. Extracts a Name argument (first parameter) and converts it to a C string
2. Extracts a text pattern (second parameter) containing LIKE wildcards (% and _)
3. Determines the string lengths for both inputs
4. Delegates the actual pattern matching to GenericMatchText with the current collation
5. Returns a boolean result indicating whether the name matches the pattern

This function is registered in the PostgreSQL system catalog as both 'namelike' and 'like' for name operands, and implements the ~~ operator for (name, text) operand types.

## Parameters / Member Variables
- : Standard PostgreSQL function calling convention containing:
  - Argument 0: Name value to match against pattern
  - Argument 1: Text pattern containing LIKE wildcards

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_NAME (extract Name argument from function call)
  - PG_GETARG_TEXT_PP (extract text argument from function call) 
  - NameStr (convert Name to C string)
  - strlen (get string length)
  - VARDATA_ANY (get text data pointer)
  - VARSIZE_ANY_EXHDR (get text size excluding header)
  - [GenericMatchText](../G/GenericMatchText.md) (perform the actual pattern matching)
  - PG_GET_COLLATION (get collation from function call context)
  - PG_RETURN_BOOL (return boolean result)
- Called from (representative examples):
  - PostgreSQL function call manager when executing LIKE with name operands
  - ~~ operator implementation for (name, text) types

## Notes and Other Information
- This function is exposed as both 'namelike' and 'like' in PostgreSQL's function catalog (pg_proc.dat)
- Implements the ~~ operator for pattern matching between name and text types
- Part of PostgreSQL's core pattern matching infrastructure alongside textlike, nameiclike, etc.
- The Name data type is specifically used for PostgreSQL object identifiers and has a maximum length limit
- Supports the full LIKE pattern syntax including % (any string) and _ (any character) wildcards
- Uses the collation specified in the function call context for locale-aware matching
- Returns LIKE_TRUE converted to PostgreSQL boolean format