# exec_command_unrestrict

## Location
src/bin/psql/command.c: 2681 - 2720

## Overview
Implements the `\unrestrict` psql command that exits restricted mode when provided with the correct key.

## Definition
```c
static backslashResult
exec_command_unrestrict(PsqlScanState scan_state, bool active_branch,
                        const char *cmd)
```

## Detailed Description
This function handles the `\unrestrict` psql meta-command which allows users to exit restricted mode by providing the correct key that was set when restricted mode was entered. Restricted mode in psql limits certain operations for security purposes, and this command provides a way to safely exit that mode with proper authentication.

The function validates that psql is currently in restricted mode, requires a key parameter, and compares the provided key against the stored restriction key. Only if the keys match exactly will restricted mode be disabled. The function includes comprehensive error handling for various failure scenarios.

## Parameters / Member Variables
- `scan_state`: PsqlScanState pointer for parsing command line options
- `active_branch`: Boolean indicating if the command should be executed or skipped
- `cmd`: String containing the command name (used for error messages)

## Dependencies
- Functions called/Symbols referenced:
  - `psql_scan_slash_option` - Parse the key parameter from command line
  - `[pfree](../p/pfree.md)` - Free the stored restriction key after successful unrestriction
  - `[ignore_slash_options](../i/ignore_slash_options.md)` - Skip option parsing when not in active branch
- Global variables accessed:
  - `restricted` - Boolean flag indicating if psql is in restricted mode
  - `restrict_key` - The key required to exit restricted mode
- Called from:
  - `[exec_command](exec_command.md)` - Main command dispatcher for `\unrestrict` command

## Notes and Other Information
- Requires psql to be currently in restricted mode to function
- The key parameter is mandatory and must exactly match the stored restriction key
- Successfully unrestricting frees the stored key and sets `restricted = false`
- Provides specific error messages for different failure conditions:
  - Missing key parameter
  - Not currently in restricted mode  
  - Incorrect key provided
- Part of psql's security framework for controlled access environments
- The restriction key is typically set when entering restricted mode via command-line options or programmatic means
- Source code location: src/bin/psql/command.c:2681-2720