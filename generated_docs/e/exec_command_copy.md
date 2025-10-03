# exec_command_copy

## Location
[src/bin/psql/command.c:715-736](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/command.c#L715-L736)

## Overview
Implements the psql  command for executing COPY operations that transfer data between PostgreSQL tables and files.

## Definition

```c
static backslashResult
exec_command_copy(PsqlScanState scan_state, bool active_branch)
```
## Detailed Description
The  function handles the  command in psql, which provides a client-side implementation of the COPY command. Unlike the server-side COPY command,  runs on the client and can access files on the client machine. The function parses the entire remaining command line as a single argument and delegates the actual copy operation to the  function.

This command is essential for data import/export operations in psql, allowing users to transfer data between PostgreSQL tables and local files with various formatting options and conditions.

## Parameters / Member Variables
- `scan_state`: Scanner state for parsing the command line arguments
- `active_branch`: Boolean indicating whether this command should be executed or just parsed
## Dependencies
- Functions called/Symbols referenced:
  - psql_scan_slash_option: Parses the entire command line using OT_WHOLE_LINE option
  - [do_copy](../d/do_copy.md): Performs the actual copy operation
  - [ignore_slash_whole_line](../i/ignore_slash_whole_line.md): Skips parsing when not in active branch
- Called from (representative examples):
  - [exec_command](exec_command.md): Main command dispatcher in psql

## Notes and Other Information
- Uses OT_WHOLE_LINE parsing mode to capture the complete COPY command syntax
- Client-side operation that can access local files, unlike server-side COPY
- Returns PSQL_CMD_SKIP_LINE on success, PSQL_CMD_ERROR on failure
- Memory management handled properly with free() for the parsed option string
- Part of the psql interactive command system located in src/bin/psql/command.c:715-736