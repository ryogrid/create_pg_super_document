# exec_command_copyright

## Location
[src/bin/psql/command.c:737-748](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/command.c#L737-L748)

## Overview
Implements the psql  command for displaying the PostgreSQL copyright notice.

## Definition

```c
static backslashResult
exec_command_copyright(PsqlScanState scan_state, bool active_branch)
```
## Detailed Description
The  function handles the  command in psql, which displays the PostgreSQL copyright and license information to the user. This is a simple informational command that delegates the actual copyright display to the  function. The function is straightforward with minimal complexity, serving primarily as a command interface wrapper.

## Parameters / Member Variables
- : Scanner state for parsing command line arguments (unused in this function)
- : Boolean indicating whether this command should be executed or just parsed

## Dependencies
- Functions called/Symbols referenced:
  - [print_copyright](../p/print_copyright.md): Displays the actual copyright notice text
- Called from (representative examples):
  - [exec_command](exec_command.md): Main command dispatcher in psql

## Notes and Other Information
- Always returns PSQL_CMD_SKIP_LINE regardless of execution success
- No error handling required as copyright display cannot fail meaningfully
- Takes no arguments and ignores any command line parameters
- Part of the psql interactive command system located in src/bin/psql/command.c:737-748