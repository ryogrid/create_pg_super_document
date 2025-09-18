# quote_file_name

## Location
[src/bin/psql/tab-complete.c:6416-6476](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/tab-complete.c#L6416-L6476)

## Overview
Quotes a filename according to SQL rules for tab completion in psql, returning a malloc'd string that handles proper escaping and quote management.

## Definition


## Detailed Description
This function is part of psql's tab completion system and handles the complex logic of properly quoting filenames that may contain special characters. It uses  to apply SQL-style quoting with single quotes, then applies sophisticated logic to determine when to strip trailing quotes based on user input context, match type, and file system properties.

The function implements several heuristics:
- Never strips trailing quotes if the user already typed one
- Strips trailing quotes for multiple/no matches (incomplete input)
- Strips trailing quotes if input was already quoted (to avoid readline bugs)
- Strips trailing quotes if the file doesn't exist or is a directory
- Handles replacement of non-single-quote characters with proper single quotes

## Parameters / Member Variables
- : The filename to be quoted
- : Type of match (e.g., SINGLE_MATCH) that affects quote stripping behavior
- : Pointer to the quote character in user input, used to determine quoting context

## Dependencies
- Functions called/Symbols referenced:
  - [quote_if_needed](quote_if_needed.md)
  - [pg_strdup](../p/pg_strdup.md)
  - [stat](../s/stat.md)
  - S_ISDIR
  - strlen
  - Assert
- Called from (representative examples):
  - Used in psql tab completion system
  - Referenced by THING_NO_SHOW and initialize_readline

## Notes and Other Information
- This is a static function specific to psql's tab completion functionality
- Integrates with GNU Readline library features like rl_completion_suppress_quote
- Handles complex edge cases around quote management that arise in interactive shell environments
- Part of a larger filename completion system that includes dequote_file_name for the reverse operation