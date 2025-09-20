# exec_command_print

## Location
[src/bin/psql/command.c:2099-2124](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/command.c#L2099-L2124)

## Overview
Implements the PostgreSQL psql  command that prints the current query buffer contents to stdout without executing the query.

## Definition

```c
static backslashResult
exec_command_print(PsqlScanState scan_state, bool active_branch,
				   PQExpBuffer query_buf, PQExpBuffer previous_buf)
```
## Detailed Description
The  function handles the  backslash command in psql, which displays the current query buffer contents. This command is useful for reviewing what query would be executed by  without actually running it. The function intelligently chooses between the current query buffer and the previous query buffer, prioritizing the current buffer if it contains data. If both buffers are empty, it displays an informative message indicating the query buffer is empty (unless quiet mode is enabled).

## Parameters / Member Variables
- : PsqlScanState pointer for scanning command line input
- : Boolean indicating if the command should be executed (used for conditional execution)
- : PQExpBuffer containing the current query being built
- : PQExpBuffer containing the previously executed query

## Dependencies
- Functions called/Symbols referenced:
  -  - Standard library function for printing strings
  -  - Flushes stdout buffer
  -  - Global setting for quiet mode
  -  - Internationalization macro for translating strings
- Called from (representative examples):
  -  - Main command dispatcher in psql

## Notes and Other Information
- Returns  to indicate the command line should be skipped from further processing
- Only executes when  is true, allowing for conditional execution in psql scripts
- Prioritizes current query buffer over previous buffer when both contain data
- Respects the quiet mode setting () when displaying empty buffer messages
- Part of psql's backslash command system located in 