# upper

## Location
[src/backend/utils/adt/oracle_compat.c:80-113](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/oracle_compat.c#L80-L113)

## Overview
The  function converts all letters in a text string to uppercase, providing case conversion functionality as part of PostgreSQL's Oracle compatibility string functions.

## Definition

```c
Datum
upper(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is a PostgreSQL built-in function that takes a text input and returns a new text value with all alphabetic characters converted to uppercase. It utilizes the database's collation settings to ensure proper case conversion for different locales and character sets. The function is implemented as part of the Oracle compatibility module, following PostgreSQL's function call conventions with proper memory management.

## Parameters / Member Variables
- : PostgreSQL function argument structure containing the input text parameter
  - Input parameter 0:  - The input string to be converted to uppercase

## Dependencies
- Functions called/Symbols referenced:
  -  - Extract text argument from function arguments
  -  - Get pointer to variable-length data
  -  - Get size of variable-length data excluding header
  -  - Core string-to-uppercase conversion function
  -  - Get collation information for proper case conversion
  -  - Convert C string to PostgreSQL text type
  -  - Free allocated memory
  -  - Return text result to PostgreSQL

- Called from (representative examples):
  - SQL queries using the  function
  - PostgreSQL query executor

## Notes and Other Information
- Located in  at lines 80-113
- Part of PostgreSQL's Oracle compatibility functions
- Properly handles memory allocation and deallocation
- Respects database collation settings for locale-aware case conversion
- Returns a new text object, leaving the original input unchanged
- Complementary function to  for case conversion operations

## Simplified Source

```c
Datum
upper(PG_FUNCTION_ARGS)
{
    // Get input text parameter
    text *in_string = PG_GETARG_TEXT_PP(0);

    // Convert string to uppercase using locale-aware function
    char *out_string = str_toupper(VARDATA_ANY(in_string),
                                   VARSIZE_ANY_EXHDR(in_string),
                                   PG_GET_COLLATION());

    // Convert result to PostgreSQL text type
    text *result = cstring_to_text(out_string);
    pfree(out_string);

    PG_RETURN_TEXT_P(result);
}
```