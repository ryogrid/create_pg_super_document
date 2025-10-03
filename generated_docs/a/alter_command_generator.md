# alter_command_generator

## Location
[src/bin/psql/tab-complete.c:5149-5159](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/tab-complete.c#L5149-L5159)

## Overview
A specialized tab completion generator function for PostgreSQL's psql that provides autocompletion suggestions specifically for commands that can follow the ALTER keyword.

## Definition

```c
static char *
alter_command_generator(const char *text, int state)
```
## Detailed Description
This function is a wrapper around  that specifically handles tab completion for ALTER statements. It filters out commands that cannot be used with ALTER by passing the  flag to exclude inappropriate completions. This ensures that users only see valid ALTER command options when pressing tab after typing "ALTER" in psql.

The function provides context-sensitive autocompletion by leveraging the shared completion infrastructure while applying ALTER-specific filtering rules to present only relevant command options to the user.

## Parameters / Member Variables
- `*text`: The partial command text that the user has typed after "ALTER"
- `state`: Call counter maintained by readline - 0 for first call, incremented on subsequent calls
## Dependencies
- Functions called/Symbols referenced:
  - : Core completion logic function
  - : Flag constant to exclude ALTER-incompatible commands
- Called from (representative examples):
  - Tab completion system via  mechanism
  -  completion matching logic (multiple locations)

## Notes and Other Information
This function enables intelligent autocompletion for ALTER commands, showing options like TABLE, INDEX, FUNCTION, DATABASE, USER, etc. The ALTER command has its own set of valid targets that may differ from CREATE and DROP commands, necessitating a separate generator with specific exclusion flags. The function is integrated into multiple parts of the completion system, indicating its widespread use in providing context-aware SQL command completion.