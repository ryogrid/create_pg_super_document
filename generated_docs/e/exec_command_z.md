# exec_command_z

## Location
src/bin/psql/command.c: 3022 - 3049

## Overview
Implements the psql \z command, which lists table access privileges and is functionally equivalent to the \dp command.

## Definition
static backslashResult exec_command_z(PsqlScanState scan_state, bool active_branch, const char *cmd)

## Detailed Description
This function handles the execution of the \z command in psql, which displays access privileges for tables, views, and other database objects. The command is essentially an alias for \dp (display privileges). The function accepts an optional pattern argument to filter the objects whose privileges are displayed. It also supports the 'S' modifier (\zS) to include system objects in the output. The actual privilege listing functionality is delegated to the permissionsList function which handles the complex formatting and querying of PostgreSQL's privilege system.

## Parameters / Member Variables
- `scan_state`: PsqlScanState pointer used for parsing command arguments
- `active_branch`: Boolean indicating whether the command should be executed (true) or just parsed (false)
- `cmd`: String containing the full command name, used to detect the 'S' modifier for system objects

## Dependencies
- Functions called/Symbols referenced:
  - psql_scan_slash_option: Parses the optional pattern argument for filtering objects
  - strchr: Checks for 'S' character in command name to determine if system objects should be shown
  - [permissionsList](../p/permissionsList.md): Core function that queries and formats privilege information
  - [ignore_slash_options](../i/ignore_slash_options.md): Skips parsing when in inactive branch
- Called from (representative examples):
  - [exec_command](exec_command.md): Main command dispatcher in psql

## Notes and Other Information
- Functionally identical to \dp command, providing an alternative short form
- Optional pattern argument supports PostgreSQL pattern matching (wildcards, schema qualification)
- The 'S' modifier (\zS) includes system catalogs and other system objects in the output
- Uses PostgreSQL's ACL (Access Control List) system to display privilege information
- Returns PSQL_CMD_SKIP_LINE on success, PSQL_CMD_ERROR on failure  
- Memory management: Properly frees the allocated pattern string
- Part of psql's comprehensive database introspection command set