# initcap

## Location
src/backend/utils/adt/oracle_compat.c: 114 - 146

## Overview
The  function capitalizes the first letter of each word in a text string while converting all other letters to lowercase, implementing proper title case formatting.

## Definition


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