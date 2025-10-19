# exec_command_lo

## Location
[src/bin/psql/command.c:1998-2075](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/command.c#L1998-L2075)

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

## Simplified Source

```c
static backslashResult exec_command_lo(PsqlScanState scan_state, bool active_branch, const char *cmd) {
    if (!active_branch) {
        ignore_slash_options(scan_state);
        return PSQL_CMD_SKIP_LINE;
    }

    // Parse up to two arguments for LO operations
    char *opt1 = psql_scan_slash_option(scan_state, OT_NORMAL, NULL, true);
    char *opt2 = psql_scan_slash_option(scan_state, OT_NORMAL, NULL, true);
    bool success = true;

    // Dispatch to specific LO operation based on command suffix
    if (strcmp(cmd + 3, "export") == 0) {
        // Export LO to file: requires both OID and filename
        if (!opt2) {
            pg_log_error("\\%s: missing required argument", cmd);
            success = false;
        } else {
            expand_tilde(&opt2);  // Handle ~ in file paths
            success = do_lo_export(opt1, opt2);
        }
    }
    else if (strcmp(cmd + 3, "import") == 0) {
        // Import file as LO: requires filename, optional OID
        if (!opt1) {
            pg_log_error("\\%s: missing required argument", cmd);
            success = false;
        } else {
            expand_tilde(&opt1);  // Handle ~ in file paths
            success = do_lo_import(opt1, opt2);
        }
    }
    else if (strcmp(cmd + 3, "list") == 0) {
        success = listLargeObjects(false);
    }
    else if (strcmp(cmd + 3, "list+") == 0) {
        success = listLargeObjects(true);  // Verbose mode
    }
    else if (strcmp(cmd + 3, "unlink") == 0) {
        // Delete LO: requires OID
        if (!opt1) {
            pg_log_error("\\%s: missing required argument", cmd);
            success = false;
        } else {
            success = do_lo_unlink(opt1);
        }
    }
    else {
        free(opt1);
        free(opt2);
        return PSQL_CMD_UNKNOWN;  // Unrecognized command
    }

    free(opt1);
    free(opt2);
    return success ? PSQL_CMD_SKIP_LINE : PSQL_CMD_ERROR;
}
```