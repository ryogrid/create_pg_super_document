# lower

## Location
src/backend/utils/adt/oracle_compat.c: 49 - 79

## Overview
The  function converts all letters in a text string to lowercase, providing case conversion functionality as part of PostgreSQL's Oracle compatibility string functions.

## Definition


## Detailed Description
The  function is a PostgreSQL built-in function that takes a text input and returns a new text value with all alphabetic characters converted to lowercase. It utilizes the database's collation settings to ensure proper case conversion for different locales and character sets. The function is implemented as part of the Oracle compatibility module, following PostgreSQL's function call conventions with proper memory management.

## Parameters / Member Variables
- : PostgreSQL function argument structure containing the input text parameter
  - Input parameter 0:  - The input string to be converted to lowercase

## Dependencies
- Functions called/Symbols referenced:
  -  - Extract text argument from function arguments
  -  - Get pointer to variable-length data
  -  - Get size of variable-length data excluding header
  -  - Core string-to-lowercase conversion function
  -  - Get collation information for proper case conversion
  -  - Convert C string to PostgreSQL text type
  -  - Free allocated memory
  -  - Return text result to PostgreSQL

- Called from (representative examples):
  - SQL queries using the  function
  - PostgreSQL query executor

## Notes and Other Information
- Located in  at lines 49-79
- Part of PostgreSQL's Oracle compatibility functions
- Properly handles memory allocation and deallocation
- Respects database collation settings for locale-aware case conversion
- Returns a new text object, leaving the original input unchanged