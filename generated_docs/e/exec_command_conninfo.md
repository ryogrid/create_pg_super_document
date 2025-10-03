# exec_command_conninfo

## Location
[src/bin/psql/command.c:671-714](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/command.c#L671-L714)

## Overview
Implements the psql  command for displaying detailed information about the current database connection.

## Definition

```c
static backslashResult
exec_command_conninfo(PsqlScanState scan_state, bool active_branch)
```
## Detailed Description
The  function handles the  command in psql, which displays comprehensive information about the current database connection including database name, username, host/address, and port. The function provides different output formats depending on the connection type (Unix socket vs network) and handles cases where host and hostaddr differ.

When connected via Unix socket, it displays the socket path, but if a hostaddr is specified, it shows the address instead. For network connections, it shows both host and address when they differ. Additionally, it displays SSL and GSS authentication information if applicable.

## Parameters / Member Variables
- `scan_state`: Scanner state for parsing command line arguments (unused in this function)
- `active_branch`: Boolean indicating whether this command should be executed or just parsed
## Dependencies
- Functions called/Symbols referenced:
  - [PQdb](../P/PQdb.md): Gets the database name from the connection
  - [PQhost](../P/PQhost.md): Gets the host name from the connection
  - [PQhostaddr](../P/PQhostaddr.md): Gets the host address from the connection
  - [PQuser](../P/PQuser.md): Gets the username from the connection
  - [PQport](../P/PQport.md): Gets the port from the connection
  - [is_unixsock_path](../i/is_unixsock_path.md): Checks if the host path is a Unix socket
  - [printSSLInfo](../p/printSSLInfo.md): Displays SSL connection information
  - [printGSSInfo](../p/printGSSInfo.md): Displays GSS authentication information
- Called from (representative examples):
  - [exec_command](exec_command.md): Main command dispatcher in psql

## Notes and Other Information
- Always returns PSQL_CMD_SKIP_LINE regardless of success/failure
- Handles both connected and disconnected states gracefully
- Provides localized output messages using the _() macro
- Distinguishes between Unix socket and network connections in output formatting
- Shows additional security information (SSL/GSS) when available
- Part of the psql interactive command system located in src/bin/psql/command.c:671-714