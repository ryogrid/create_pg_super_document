# text_right

## Location
src/backend/utils/adt/varlena.c: 5562 - 5582

## Overview
The  function extracts the rightmost n characters from a text string, with support for negative values to return all but the first |n| characters.

## Definition


## Detailed Description
This function implements PostgreSQL's  SQL function for text data types. It handles multibyte character encoding properly by using PostgreSQL's multibyte string functions. The function supports two modes of operation:
- When n is positive: returns the last n characters from the string
- When n is negative: returns all characters except the first |n| characters

The implementation uses PostgreSQL's multibyte-aware functions to ensure correct handling of Unicode and other multibyte character sets.

## Parameters / Member Variables
- : The input text string (retrieved via )
- : Number of characters to extract from the right (retrieved via )
  - If positive: extract last n characters
  - If negative: extract all but first |n| characters

## Dependencies
- Functions called/Symbols referenced:
  -  - Extract text argument from function call
  -  - Extract integer argument from function call
  -  - Get pointer to variable-length data
  -  - Get size of variable-length data excluding header
  -  - Get multibyte string length
  -  - Clip multibyte string to specified character count
  -  - Convert C string to PostgreSQL text type
  -  - Return text value from function
- Called from (representative examples):
  - SQL RIGHT() function invocations

## Notes and Other Information
- Located in 
- Properly handles multibyte character encodings (UTF-8, etc.)
- The function uses character-based positioning rather than byte-based positioning
- Memory management is handled through PostgreSQL's memory context system
- Returns a new text object containing the extracted substring