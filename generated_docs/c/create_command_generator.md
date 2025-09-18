# create_command_generator

## Location
[src/bin/psql/tab-complete.c:5131-5139](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/tab-complete.c#L5131-L5139)

## Overview
A specialized tab completion generator function for PostgreSQL's psql that provides autocompletion suggestions specifically for commands that can follow the CREATE keyword.

## Definition


## Detailed Description
This function is a thin wrapper around  that specifically handles tab completion for CREATE statements. It filters out commands that cannot be used with CREATE by passing the  flag to exclude inappropriate completions. This ensures that users only see valid CREATE command options when pressing tab after typing "CREATE" in psql.

The function follows the standard readline completion generator interface, taking the partial text and state parameter, and returning matching command names one at a time until exhausted.

## Parameters / Member Variables
- : The partial command text that the user has typed after "CREATE"
- : Call counter maintained by readline - 0 for first call, incremented on subsequent calls

## Dependencies
- Functions called/Symbols referenced:
  - : Core completion logic function
  - : Flag constant to exclude CREATE-incompatible commands
- Called from (representative examples):
  - Tab completion system via  mechanism
  -  completion matching logic

## Notes and Other Information
This function is part of psql's context-sensitive tab completion system. It ensures that after typing "CREATE", users only see completions for valid CREATE commands like TABLE, INDEX, FUNCTION, etc., while filtering out commands that don't make sense in this context. The function leverages the shared completion infrastructure while providing CREATE-specific filtering.