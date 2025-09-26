# array_to_text

## Location
[src/backend/utils/adt/varlena.c:4766-4781](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varlena.c#L4766-L4781)

## Overview
PostgreSQL built-in function that concatenates array elements into a single text string using a specified field separator.

## Definition

```c
Datum
array_to_text(PG_FUNCTION_ARGS)
```
## Detailed Description
This function serves as the main entry point for the array_to_text SQL function in PostgreSQL. It takes an array and a field separator as arguments and converts the array elements to their string representations, then concatenates them using the provided separator. The function is a thin wrapper around array_to_text_internal, handling argument extraction and result formatting according to PostgreSQL's function calling conventions. NULL array elements are simply skipped in the concatenation process (no null replacement string is used).

## Parameters / Member Variables
- Function arguments accessed via PG_FUNCTION_ARGS macro:
  - Argument 0: ArrayType pointer - the input array to be converted to text
  - Argument 1: text pointer - the field separator string to use between array elements

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_ARRAYTYPE_P (to extract array argument)
  - [text_to_cstring](../t/text_to_cstring.md) (to convert separator text to C string)
  - [array_to_text_internal](array_to_text_internal.md) (performs the actual array-to-text conversion)
  - PG_RETURN_TEXT_P (to return the result as PostgreSQL text type)
- Called from:
  - SQL queries using the array_to_text() function

## Notes and Other Information
This function corresponds to the two-parameter version of PostgreSQL's array_to_text() SQL function. The function signature follows PostgreSQL's V1 calling convention using the PG_FUNCTION_ARGS macro. Unlike array_to_text_null, this function does not provide a mechanism to replace NULL array elements with a custom string - NULL elements are simply omitted from the result. The actual concatenation logic is delegated to array_to_text_internal with a NULL parameter for the null replacement string.