# pg_append_history

## Location
src/bin/psql/input.c: 113 - 134

## Overview
Appends a line to the history buffer, ensuring proper newline termination for command history tracking in psql.

## Definition
```c
void pg_append_history(const char *s, PQExpBuffer history_buf)
```

## Detailed Description
This function is responsible for building up the command history buffer by appending input lines. It operates only when readline functionality is available (USE_READLINE is defined) and history tracking is enabled (useHistory is true). The function ensures that each line in the history buffer is properly terminated with a newline character, adding one if the input string doesn't already end with a newline.

The function performs a simple but important role in psql's history management by accumulating command lines as they are entered, which can later be saved to the history file or used for other history-related operations.

## Parameters / Member Variables
- `s`: The input string to append to the history buffer (can be NULL, in which case no action is taken)
- `history_buf`: The PQExpBuffer that accumulates history lines

## Dependencies
- Functions called/Symbols referenced:
  - USE_READLINE (preprocessor macro)
  - appendPQExpBufferStr
  - appendPQExpBufferChar
  - strlen
- Called from (representative examples):
  - MainLoop (multiple locations)

## Notes and Other Information
- Only functions when USE_READLINE is defined and useHistory is enabled
- Automatically adds a trailing newline if the input string doesn't have one
- Safely handles NULL input strings by checking for validity before processing
- Part of psql's command history management system
- The history buffer can be later processed by pg_send_history() to save to file