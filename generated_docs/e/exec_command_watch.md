# exec_command_watch

## Location
src/bin/psql/command.c: 2853 - 2999

## Overview
Implements the psql \watch command, which repeatedly executes a query at specified intervals with optional iteration count and minimum row filtering.

## Definition
static backslashResult exec_command_watch(PsqlScanState scan_state, bool active_branch, PQExpBuffer query_buf, PQExpBuffer previous_buf)

## Detailed Description
This function handles the execution of the \watch command in psql, which provides a mechanism to repeatedly execute the same query at regular intervals. The function supports flexible parameter specification including named parameters (i=interval, c=count, m=min_rows) and positional parameters. It includes comprehensive argument parsing with validation for numeric values and duplicate parameter detection. The function can optionally stop after a specified number of iterations and can filter results based on minimum row count. When no query is present in the current buffer, it automatically recalls the previous query.

## Parameters / Member Variables
- `scan_state`: PsqlScanState pointer used for parsing command arguments
- `active_branch`: Boolean indicating whether the command should be executed (true) or just parsed (false)
- `query_buf`: PQExpBuffer containing the current query to be watched; cleared after execution
- `previous_buf`: PQExpBuffer containing the previous query as fallback

## Dependencies
- Functions called/Symbols referenced:
  - psql_scan_slash_option: Parses command arguments with flexible name=value syntax
  - strtod: Converts string interval values to double precision
  - strtoint: Converts string count/min_rows values to integers
  - [copy_previous_query](../c/copy_previous_query.md): Recalls previous query when current buffer is empty
  - [do_watch](../d/do_watch.md): Core execution function that performs the repeated query execution
  - [resetPQExpBuffer](../r/resetPQExpBuffer.md): Clears the query buffer after execution
  - psql_scan_reset: Resets the scanner state
  - [ignore_slash_options](../i/ignore_slash_options.md): Skips parsing when in inactive branch
- Called from (representative examples):
  - [exec_command](exec_command.md): Main command dispatcher in psql

## Notes and Other Information
- Supports multiple parameter formats: unlabeled interval, i=/interval=, c=/count=, m=/min_rows=
- Default interval is 2 seconds, iteration count is unlimited (0), no minimum row filtering
- Includes comprehensive error checking for duplicate parameters and invalid values
- Automatically uses previous query if current buffer is empty (similar to \g behavior)
- Clears query buffer after execution (similar to \r reset behavior)
- Uses errno checking for robust numeric conversion error detection
- Returns PSQL_CMD_SKIP_LINE on success, PSQL_CMD_ERROR on parsing/validation failures
- Memory management: Properly frees allocated option strings during parsing