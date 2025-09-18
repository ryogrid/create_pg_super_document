# dequote_file_name

## Location
src/bin/psql/tab-complete.c: 6477 - 6513

## Overview
Removes quotes from a filename string if it's quoted, handling proper unescaping for psql tab completion functionality.

## Definition


## Detailed Description
This function is the counterpart to quote_file_name in psql's tab completion system. It takes a potentially quoted filename and removes the quotes while properly handling escape sequences. The function uses PostgreSQL's strtokx() tokenizer to parse the quoted string according to SQL quoting rules.

The function handles two scenarios:
- When quote_char is set (typically single quote), it reconstructs the full quoted string by prepending the quote character before parsing
- When quote_char is not set, it processes the filename directly
- Uses strtokx with SQL-style quote handling and escape character processing
- Returns a malloc'd copy of the unquoted result for readline compatibility

## Parameters / Member Variables  
- : The filename string that may contain quotes and escape sequences
- : The quote character used (typically single quote '\'') or '\0' if none

## Dependencies
- Functions called/Symbols referenced:
  - pg_malloc
  - [strtokx](../s/strtokx.md) 
  - strcpy
  - strlen
  - free
  - [pg_strdup](../p/pg_strdup.md)
  - Assert
- Called from (representative examples):
  - Used in psql tab completion system
  - Referenced by THING_NO_SHOW and initialize_readline

## Notes and Other Information
- This is a static function specific to psql's tab completion functionality
- Works in conjunction with quote_file_name to provide complete quote/unquote functionality
- Uses PostgreSQL's strtokx tokenizer which handles SQL-style escaping rules
- Returns malloc'd memory that the caller (typically readline) is expected to free
- Handles edge case of empty strings by returning the original fname pointer
- Part of the broader filename completion infrastructure in psql