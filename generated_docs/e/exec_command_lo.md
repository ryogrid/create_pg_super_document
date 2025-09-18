# exec_command_lo

## Location
src/bin/psql/command.c: 1998 - 2075

## Overview
Handles various large object (LO) operations in psql, including import, export, list, and unlink commands for PostgreSQL large objects.

## Definition
```c
static backslashResult exec_command_lo(PsqlScanState scan_state, bool active_branch, const char *cmd)
```

## Detailed Description
The `exec_command_lo` function implements the family of \\lo_* commands in psql that provide large object management capabilities. Large objects in PostgreSQL are a facility for storing data that is too large to be stored directly in table fields, providing stream-style access to user data.

The function dispatches to specific large object operations based on the command suffix:
- `\\lo_export`: Exports a large object to a file on the client filesystem
- `\\lo_import`: Imports a file from the client filesystem as a large object  
- `\\lo_list` and `\\lo_list+`: Lists large objects (verbose mode with +)
- `\\lo_unlink`: Removes a large object from the database

The function performs proper argument validation, ensuring required parameters are provided for each operation. It also handles file path expansion using tilde (~) notation and manages memory cleanup for parsed options.

## Parameters / Member Variables
- `scan_state`: PsqlScanState pointer for parsing command-line options and arguments
- `active_branch`: Boolean indicating whether this command should execute or be skipped (for conditional execution)
- `cmd`: String containing the full command name (e.g., "lo_export", "lo_import") used to determine the specific operation

## Dependencies
- Functions called/Symbols referenced:
  - psql_scan_slash_option
  - strcmp
  - pg_log_error
  - [expand_tilde](expand_tilde.md)
  - [do_lo_export](../d/do_lo_export.md)
  - [do_lo_import](../d/do_lo_import.md)
  - [listLargeObjects](../l/listLargeObjects.md)
  - [do_lo_unlink](../d/do_lo_unlink.md)
  - free
  - [ignore_slash_options](../i/ignore_slash_options.md)
- Called from (representative examples):
  - [exec_command](exec_command.md)

## Notes and Other Information
- Returns PSQL_CMD_SKIP_LINE on success, PSQL_CMD_ERROR on failure, or PSQL_CMD_UNKNOWN for unrecognized commands
- Supports PostgreSQL's large object facility for managing binary data too large for regular table storage
- Performs argument validation and provides appropriate error messages for missing required parameters
- Uses expand_tilde() to handle ~ (home directory) notation in file paths
- Integrates with conditional execution system via active_branch parameter
- Memory management includes proper cleanup of dynamically allocated option strings
- The + suffix on lo_list provides verbose output with additional metadata
- All file operations work on the client side, transferring data between server large objects and local files