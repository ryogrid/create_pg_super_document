# exec_command_edit

## Location
src/bin/psql/command.c: 1081 - 1176

## Overview
Implements the \e and \edit commands in psql for editing the current query buffer or a specified file using an external editor, then loading the result back into the query buffer.

## Definition
```c
static backslashResult exec_command_edit(PsqlScanState scan_state, bool active_branch, PQExpBuffer query_buf, PQExpBuffer previous_buf)
```

## Detailed Description
This function handles the \e (\edit) command which allows users to edit SQL queries using their preferred external editor. It supports multiple usage patterns: editing the current query buffer, editing a specific file and loading it into the buffer, or editing with a specific line number for cursor positioning. The function intelligently handles argument parsing where a single numeric argument is treated as a line number rather than a filename. It integrates with the psql query buffer system and handles various error conditions gracefully.

## Parameters / Member Variables
- `scan_state`: PsqlScanState pointer for parsing optional filename and line number arguments
- `active_branch`: Boolean indicating whether this command should be executed or ignored due to conditional logic
- `query_buf`: PQExpBuffer containing the current query buffer that will be edited
- `previous_buf`: PQExpBuffer containing the previous query for recall if current buffer is empty

## Dependencies
- Functions called/Symbols referenced:
  - psql_scan_slash_option
  - expand_tilde
  - canonicalize_path_enc
  - copy_previous_query
  - do_edit
  - resetPQExpBuffer
  - ignore_slash_options
  - pg_log_error, atoi, strspn, strlen
- Called from (representative examples):
  - exec_command (main command dispatcher)

## Notes and Other Information
- Supports syntax: \e [filename] [line_number] or \e [line_number]
- If only a numeric argument is provided, it's treated as a line number for the current buffer
- Uses expand_tilde to handle ~ in file paths
- Calls canonicalize_path_enc to normalize file paths according to client encoding
- If query buffer is empty, recalls previous query for editing
- Returns PSQL_CMD_NEWEDIT on successful edit to trigger query buffer refresh
- Resets query buffer on error to prevent corrupted state
- Essential for interactive SQL development workflow in psql