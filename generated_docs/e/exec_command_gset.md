# exec_command_gset

## Location
[src/bin/psql/command.c:1634-1662](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/command.c#L1634-L1662)

## Overview
Implements the \gset command in psql, which stores query result values into psql variables with optional prefix naming.

## Definition

```c
static backslashResult
exec_command_gset(PsqlScanState scan_state, bool active_branch)
```
## Detailed Description
This function handles the \gset backslash command which stores the results of the next query into psql variables. It accepts an optional prefix argument that will be prepended to variable names. If no prefix is provided, an empty string is used as the prefix. The function sets up the gset mechanism by storing the prefix and setting a flag. The actual variable assignment from query results is handled elsewhere in the query processing pipeline.

## Parameters / Member Variables
- `scan_state`: Scanner state for reading the optional prefix argument from input stream
- `active_branch`: Whether to actually set up gset mode (true) or just parse arguments (false)
## Dependencies
- Functions called/Symbols referenced:
  - psql_scan_slash_option
  - [pg_strdup](../p/pg_strdup.md)
  - [ignore_slash_options](../i/ignore_slash_options.md)
  - PSQL_CMD_SKIP_LINE
  - PSQL_CMD_SEND
- Called from (representative examples):
  - [exec_command](exec_command.md)

## Notes and Other Information
- Takes an optional prefix argument for variable naming
- Sets pset.gset_prefix to the provided prefix or empty string if none given
- The prefix must be non-NULL to trigger the variable storing mechanism
- When not in active_branch, uses ignore_slash_options to skip argument parsing
- Returns PSQL_CMD_SEND to indicate the next query should be processed with gset mode
- The gset_prefix memory is freed later in the query processing cycle

## Simplified Source

```c
static backslashResult exec_command_gset(PsqlScanState scan_state, bool active_branch) {
    backslashResult status = PSQL_CMD_SKIP_LINE;

    if (active_branch) {
        // Parse optional prefix argument
        char *prefix = psql_scan_slash_option(scan_state, OT_NORMAL, NULL, false);

        if (prefix) {
            pset.gset_prefix = prefix;
        } else {
            // Use empty string prefix to trigger variable storing
            pset.gset_prefix = pg_strdup("");
        }

        status = PSQL_CMD_SEND;
    } else {
        // Not in active branch - consume arguments
        ignore_slash_options(scan_state);
    }

    return status;
}
```