# p_isEOF

## Location
src/backend/tsearch/wparser_def.c: 474 - 480

## Overview
A static utility function that checks whether the text search parser has reached the end of the input string during parsing operations.

## Definition


## Detailed Description
The  function is a helper utility in PostgreSQL's text search parser that determines if the parser has reached the end of the input string. It performs this check by examining two conditions: whether the current byte position has reached the end of the string length, or whether the character length is zero. The function is designed to be used internally within the text search parsing framework to control parsing loops and determine when to stop processing input text.

This function is part of the word parser definition module () which handles the tokenization and parsing of text for full-text search operations in PostgreSQL.

## Parameters / Member Variables
- : Pointer to a TParser structure that contains the current parsing state, including position information and string length

## Dependencies
- Functions called/Symbols referenced:
  - TParser (structure type)
  - Assert (assertion macro)
- Called from (representative examples):
  - p_isspecial (extensively used throughout the function)
  - _make_compiler_happy

## Notes and Other Information
- This is a static function, meaning it's only accessible within the same compilation unit
- The function uses an Assert to ensure the parser state is valid before checking EOF conditions
- Returns 1 if at EOF, 0 otherwise
- Critical for controlling parsing loops in the text search tokenization process
- Used extensively by the p_isspecial function which handles special character detection during parsing