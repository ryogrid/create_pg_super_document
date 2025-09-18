# exec_command_C

## Location
[src/bin/psql/command.c:521-553](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/command.c#L521-L553)

## Overview
exec_command_C implements the \C backslash command that overrides the table title in PostgreSQL psql output formatting (formerly used to change HTML caption).

## Definition


## Detailed Description
exec_command_C sets or clears the table title that appears above query result tables in psql output. The function accepts an optional argument that becomes the new title - if no argument is provided (or an empty string), the title is cleared. The implementation uses psql_scan_slash_option() with the 'true' parameter to allow empty strings, then calls do_pset() to update the "title" print setting.

When active_branch is false (inside a false \if block), the function calls ignore_slash_options() to consume and discard any arguments without processing them. According to the comment, this command formerly changed HTML captions but now serves the broader purpose of setting table titles for various output formats.

## Parameters / Member Variables
- `scan_state`: Lexer working state used to parse the optional title argument
- `active_branch`: Boolean indicating whether the command should actually execute (false when inside a false \if block)

## Dependencies
- Functions called/Symbols referenced:
  - psql_scan_slash_option
  - do_pset
  - [ignore_slash_options](../i/ignore_slash_options.md)
  - OT_NORMAL (option type constant)
- Called from (representative examples):
  - [exec_command](exec_command.md) (src/bin/psql/command.c:333)

## Notes and Other Information
- Returns PSQL_CMD_SKIP_LINE on success, PSQL_CMD_ERROR on failure
- Accepts one optional argument: the new table title text
- Empty string or no argument clears the current title
- Uses the same underlying mechanism (do_pset) as \pset title command
- Properly handles conditional execution by ignoring arguments when not in active branch
- The title will appear above result tables in subsequent query output
- Originally designed for HTML caption functionality but now applies to various output formats
- Memory is properly managed with free() call after processing the argument