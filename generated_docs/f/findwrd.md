# findwrd

## Location
src/backend/tsearch/dict_synonym.c: 44 - 84

## Overview
Finds the next whitespace-delimited word within an input string, handling multibyte characters and special prefix flags.

## Definition


## Detailed Description
This function parses text to extract individual words separated by whitespace. It supports PostgreSQL's multibyte character handling and can detect prefix search operators (indicated by a trailing '*' character). The function skips leading whitespace, identifies word boundaries, and handles special cases for text search operations.

Key behaviors:
- Skips leading whitespace using  for proper character classification
- Uses  for correct multibyte character handling
- Detects '*' suffix to set TSL_PREFIX flag for prefix search operations
- Returns NULL for empty input or lines containing only whitespace

## Parameters / Member Variables
- : Input string to parse for the next word
- : Output pointer set to the character immediately after the found word
- : Optional output parameter set to TSL_PREFIX if word ends with '*', otherwise 0

## Dependencies
- Functions called/Symbols referenced:
  - [t_isspace](../t/t_isspace.md)
  - [pg_mblen](../p/pg_mblen.md)
  - t_iseq
  - TSL_PREFIX
- Called from (representative examples):
  - [dsynonym_init](../d/dsynonym_init.md) (twice - for parsing input and output words from synonym file)

## Notes and Other Information
- This is a static function used internally within the synonym dictionary module
- Handles multibyte characters correctly for international text processing
- The '*' character detection is specifically for text search prefix matching functionality
- Returns start of word and sets end pointer for efficient string processing without copying