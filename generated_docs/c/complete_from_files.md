# complete_from_files

## Location
src/bin/psql/tab-complete.c: 5798 - 5897

## Overview
Provides filename completion functionality for psql tab completion, handling proper quoting and unquoting of filenames based on the command context and user input.

## Definition


## Detailed Description
This function wraps rl_filename_completion_function() to handle filename completion with proper quoting for psql commands. It strips quotes from input before searching for matches and re-quotes results as needed based on the consuming command's requirements. The function supports two different implementation paths: one using readline's filename quoting hooks (when available) and a fallback implementation that manually handles quoting/unquoting.

For directories, it replaces trailing quotes with slashes for better usability. The function is aware of escape characters and force-quote settings that vary between different psql commands (e.g., \copy has no escape character while other backslash commands use backslash as escape).

## Parameters / Member Variables
- : The input text being completed (potentially quoted filename fragment)
- : Completion state (0 for first call, non-zero for subsequent calls)

## Dependencies
- Functions called/Symbols referenced:
  - rl_filename_completion_function
  - [strtokx](../s/strtokx.md)
  - [quote_if_needed](../q/quote_if_needed.md)
  - S_ISDIR
  - [stat](../s/stat.md)
- Called from (representative examples):
  - HeadMatchesCS (multiple locations in tab-complete.c)
  - THING_NO_SHOW completion generator

## Notes and Other Information
The function behavior depends on global variables completion_charp (escape character) and completion_force_quote (whether to force quotes). It handles both USE_FILENAME_QUOTING_FUNCTIONS and fallback modes for different readline library versions. Special handling exists for directory completion by replacing trailing quotes with forward slashes.