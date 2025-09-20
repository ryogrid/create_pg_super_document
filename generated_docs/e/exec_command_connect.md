# exec_command_connect

## Location
[src/bin/psql/command.c:554-606](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/command.c#L554-L606)

## Overview
Implements the psql  or  command for establishing a new database connection with specified parameters.

## Definition

```c
enum trivalue reuse_previous = TRI_DEFAULT;
```
## Detailed Description
The  function handles the  (connect) command in psql, which allows users to connect to a PostgreSQL database using specified connection parameters. The function supports an optional  flag and accepts up to four connection parameters: database name, username, host, and port. Parameters can be specified as '-' to use current values or omitted entirely.

The function parses command arguments sequentially, handles the optional reuse-previous flag, and delegates the actual connection establishment to the  function. It provides comprehensive parameter handling with flexible syntax allowing users to specify only the parameters they want to change.

## Parameters / Member Variables
- : Scanner state for parsing the remaining command line arguments
- : Boolean indicating whether this command should be executed or just parsed (for conditional execution)

## Dependencies
- Functions called/Symbols referenced:
  - [read_connect_arg](../r/read_connect_arg.md): Reads connection arguments from the command line
  - [ParseVariableBool](../P/ParseVariableBool.md): Parses boolean values for the reuse-previous option
  - [do_connect](../d/do_connect.md): Performs the actual database connection
  - [ignore_slash_options](../i/ignore_slash_options.md): Skips parsing when not in active branch
- Called from (representative examples):
  - [exec_command](exec_command.md): Main command dispatcher in psql

## Notes and Other Information
- Supports flexible parameter specification where '-' means "use current value"
- The  option controls whether to reuse previous connection parameters
- Returns PSQL_CMD_SKIP_LINE on success, PSQL_CMD_ERROR on failure
- Memory management is handled carefully with proper free() calls for allocated arguments
- Part of the psql interactive command system located in src/bin/psql/command.c:554-606