# exec_command_s

## Location
[src/bin/psql/command.c:2394-2420](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/command.c#L2394-L2420)

## Overview
Implements the psql \s backslash command that saves command history to a file or displays it on screen.

## Definition

```c
static backslashResult
exec_command_s(PsqlScanState scan_state, bool active_branch)
```
## Detailed Description
The  function handles the execution of the \s backslash command in psql, which manages command history display and saving. The command can operate in two modes: if no filename is provided, it displays the command history on screen (potentially using a pager); if a filename is provided, it saves the history to that file.

When executed in an active branch, the function first attempts to parse an optional filename argument. If a filename is provided, it undergoes tilde expansion (converting ~ to the user's home directory path) before being passed to . The function provides user feedback when successfully writing to a file (unless in quiet mode) and adds a newline when displaying to screen. The success or failure of the history operation determines the return value.

## Parameters / Member Variables
- `scan_state`: Scanner state object used to parse the optional filename argument from the command line
- `active_branch`: Boolean flag indicating whether this command should actually execute (used for conditional execution in psql)
## Dependencies
- Functions called/Symbols referenced:
  - psql_scan_slash_option
  - [expand_tilde](expand_tilde.md)
  - [printHistory](../p/printHistory.md)
  - [ignore_slash_options](../i/ignore_slash_options.md)
  - printf
  - putchar
  - free
  - [PsqlScanState](../P/PsqlScanState.md) (type)
  - [backslashResult](../b/backslashResult.md) (return type)  
  - OT_NORMAL (option type)
  - PSQL_CMD_SKIP_LINE (success return value)
  - PSQL_CMD_ERROR (error return value)
- Called from (representative examples):
  - [exec_command](exec_command.md)
  - EditableObjectType (indirectly through command dispatch)

## Notes and Other Information
- This function is part of psql's history management system, providing both display and save functionality
- The function supports tilde expansion in filenames, allowing users to use ~ for home directory references
- When no filename is provided, history is displayed on screen with potential pager usage based on  settings
- Success feedback is provided when writing to files, but only when not in quiet mode ()
- Memory management includes proper cleanup of the filename string with 
- The function properly handles conditional execution by ignoring options when not in an active branch
- Returns  if the history operation fails, otherwise returns 
- A newline is explicitly added when displaying history to screen (when no filename is provided)