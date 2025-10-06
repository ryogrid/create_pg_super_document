# initcap

## Location
[src/backend/utils/adt/oracle_compat.c:114-146](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/oracle_compat.c#L114-L146)

## Overview
The  function capitalizes the first letter of each word in a text string while converting all other letters to lowercase, implementing proper title case formatting.

## Definition

```c
Datum
initcap(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is a PostgreSQL built-in function that performs title case conversion on text input. It capitalizes the first letter of each word while converting all other letters to lowercase. Words are defined as sequences of alphanumeric characters delimited by non-alphanumeric characters. The function utilizes the database's collation settings to ensure proper case conversion for different locales and character sets. It is implemented as part of the Oracle compatibility module.

## Parameters / Member Variables
- : PostgreSQL function argument structure containing the input text parameter
  - Input parameter 0:  - The input string to be converted to initial capitals (title case)

## Dependencies
- Functions called/Symbols referenced:
  -  - Extract text argument from function arguments
  -  - Get pointer to variable-length data
  -  - Get size of variable-length data excluding header
  -  - Core string initial capitalization function
  -  - Get collation information for proper case conversion
  -  - Convert C string to PostgreSQL text type
  -  - Free allocated memory
  -  - Return text result to PostgreSQL

- Called from (representative examples):
  - SQL queries using the  function
  - PostgreSQL query executor

## Notes and Other Information
- Located in  at lines 114-146
- Part of PostgreSQL's Oracle compatibility functions
- Properly handles memory allocation and deallocation
- Word boundaries are defined by non-alphanumeric characters
- Respects database collation settings for locale-aware case conversion
- Returns a new text object, leaving the original input unchanged
- Useful for formatting names, titles, and other text requiring title case

## Simplified Source

```c
Datum
initcap(PG_FUNCTION_ARGS)
{
    // Get input text parameter
    text *in_string = PG_GETARG_TEXT_PP(0);

    // Capitalize first letter of each word using locale-aware function
    char *out_string = str_initcap(VARDATA_ANY(in_string),
                                   VARSIZE_ANY_EXHDR(in_string),
                                   PG_GET_COLLATION());

    // Convert result to PostgreSQL text type
    text *result = cstring_to_text(out_string);
    pfree(out_string);

    PG_RETURN_TEXT_P(result);
}
```