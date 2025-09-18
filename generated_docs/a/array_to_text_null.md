# array_to_text_null

## Location
[src/backend/utils/adt/varlena.c:4782-4807](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varlena.c#L4782-L4807)

## Overview
PostgreSQL built-in function that concatenates array elements into a single text string using a specified field separator, with the ability to replace NULL array elements with a custom null representation string.

## Definition


## Detailed Description
This function provides an extended version of array_to_text that supports NULL element handling. It takes three arguments: an array, a field separator, and an optional null replacement string. When array elements are NULL, they are replaced with the provided null string before concatenation. The function is marked as 'not strict' in the PostgreSQL system, meaning it must explicitly handle NULL input arguments rather than automatically returning NULL when any argument is NULL. This allows for more flexible NULL handling - specifically, the third argument (null replacement string) can be NULL, in which case NULL array elements are treated the same as in the basic array_to_text function.

## Parameters / Member Variables
- Function arguments accessed via PG_FUNCTION_ARGS macro:
  - Argument 0: ArrayType pointer - the input array to be converted to text (required, NULL causes function to return NULL)
  - Argument 1: text pointer - the field separator string (required, NULL causes function to return NULL)  
  - Argument 2: text pointer - the null replacement string (optional, NULL means no null replacement)

## Dependencies
- Functions called/Symbols referenced:
  - PG_ARGISNULL (to check for NULL arguments)
  - PG_GETARG_ARRAYTYPE_P (to extract array argument)
  - text_to_cstring (to convert text arguments to C strings)
  - [array_to_text_internal](array_to_text_internal.md) (performs the actual array-to-text conversion)
  - PG_RETURN_TEXT_P (to return the result as PostgreSQL text type)
  - PG_RETURN_NULL (to return NULL when required arguments are missing)
- Called from:
  - SQL queries using the three-parameter array_to_text() function

## Notes and Other Information
This function corresponds to the three-parameter version of PostgreSQL's array_to_text() SQL function. The 'not strict' behavior is important because it allows the third parameter to be NULL while still processing the array, giving users flexibility in NULL handling. The explicit NULL checking for the first two arguments ensures that essential inputs (array and separator) are present before proceeding. The function delegates the actual work to array_to_text_internal, which contains the core concatenation logic shared between the two-parameter and three-parameter variants.