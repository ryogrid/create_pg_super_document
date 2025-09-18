# exec_command_list

## Location
src/bin/psql/command.c: 1970 - 1997

## Overview
Handles the \\l command in psql, which lists all databases in the PostgreSQL server with optional pattern matching and verbose output.

## Definition
```c
static backslashResult exec_command_list(PsqlScanState scan_state, bool active_branch, const char *cmd)
```

## Detailed Description
The `exec_command_list` function implements the \\l (list databases) command functionality in psql. When executed, it displays a list of all databases available on the connected PostgreSQL server. The function supports pattern matching to filter databases and verbose mode to show additional details.

The function respects conditional execution by only performing the database listing when `active_branch` is true. When in an inactive conditional branch, it merely consumes any provided options without executing the actual listing operation.

The command supports:
- Pattern matching using standard PostgreSQL wildcards
- Verbose mode (\\l+) which shows additional database information like size, tablespace, and access privileges
- Proper option parsing and memory management

## Parameters / Member Variables
- `scan_state`: PsqlScanState pointer for parsing command-line options and arguments
- `active_branch`: Boolean indicating whether this command should execute or be skipped (for conditional execution)
- `cmd`: String containing the actual command that may include modifiers like + for verbose mode

## Dependencies
- Functions called/Symbols referenced:
  - psql_scan_slash_option
  - strchr
  - [listAllDbs](../l/listAllDbs.md)
  - free
  - [ignore_slash_options](../i/ignore_slash_options.md)
- Called from (representative examples):
  - [exec_command](exec_command.md)

## Notes and Other Information
- Returns PSQL_CMD_SKIP_LINE on success or PSQL_CMD_ERROR on failure  
- Part of psql's database introspection commands
- Supports the + modifier for verbose output showing additional database metadata
- Handles memory management by freeing the pattern string after use
- Integrates with psql's conditional execution system via active_branch parameter
- Uses ignore_slash_options() to consume unused arguments when not executing
- The pattern parameter supports PostgreSQL's standard wildcards for filtering database names